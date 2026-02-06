import streamlit as st
import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
import sys
import subprocess
import traceback

# --- НАСТРОЙКИ ---
# Ставим False, чтобы собирать все найденные записи
TEST_LIMIT_2 = False 

# Фикс для Windows (на всякий случай)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

nest_asyncio.apply()

# Установка браузера
@st.cache_resource
def install_browser():
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    except Exception as e:
        print(f"Error installing browser: {e}")

install_browser()

st.set_page_config(page_title="Yandex Proxy Parser", page_icon="🕵️", layout="wide")
st.title("🕵️ Парсер с Прокси (AstroProxy)")

# --- ПРОВЕРКА НАСТРОЕК ПРОКСИ ---
def get_proxy_config():
    """Читает прокси из st.secrets"""
    if "proxy" in st.secrets:
        # Формируем строку подключения
        return {
            "server": f"http://{st.secrets['proxy']['server']}",
            "username": st.secrets['proxy']['username'],
            "password": st.secrets['proxy']['password']
        }
    else:
        return None

# --- ФУНКЦИИ ПАРСИНГА ---

async def scrape_listing(context, query, status_log):
    page = await context.new_page()
    status_log.info(f"🔍 [Прокси] Захожу на Яндекс...")
    
    try:
        # Проверка IP (оставляем как было)
        try:
            await page.goto("http://lumtest.com/myip.json", timeout=15000)
            content = await page.content()
            if "ip" in content:
                status_log.success("✅ Прокси работает! IP скрыт.")
        except:
            status_log.warning("⚠️ Проверка IP не прошла, но пробуем дальше...")

        # --- ЗАХОД НА ЯНДЕКС ---
        try:
            # Даем 60 секунд на загрузку
            await page.goto("https://yandex.ru/maps", timeout=60000, wait_until="domcontentloaded")
            
            # !!! СРАЗУ ПОКАЗЫВАЕМ СКРИНШОТ !!!
            # Это покажет, загрузилась карта или капча
            screenshot = await page.screenshot()
            st.image(screenshot, caption="Что видит бот прямо сейчас", width=500)
            
        except Exception as e:
            status_log.error(f"Не удалось открыть yandex.ru: {e}")
            return []
        
        # Ждем строку поиска
        try:
            status_log.write("⏳ Ищу поле поиска...")
            await page.wait_for_selector("input.input__control", timeout=20000)
        except:
            status_log.error("⚠️ Не вижу строку поиска! Скорее всего на скриншоте выше — КАПЧА.")
            return []

        await page.fill("input.input__control", query)
        await page.keyboard.press("Enter")
        
        status_log.write("⏳ Жду результаты...")
        list_selector = ".search-list-view__list"
        await page.wait_for_selector(list_selector, timeout=25000)
        await page.click(list_selector)
        
    except Exception as e:
        status_log.error(f"Ошибка поиска: {e}")
        return []

    unique_items = {}
    stuck_counter = 0
    last_len = 0
    
    max_scrolls = 40 
    bar = st.progress(0, text="Скроллинг...")

    for i in range(max_scrolls):
        cards = await page.query_selector_all("li.search-snippet-view")
        if not cards: cards = await page.query_selector_all(".search-business-snippet-view")

        for card in cards:
            try:
                link_el = await card.query_selector("a")
                link = "https://yandex.ru" + await link_el.get_attribute("href") if link_el else ""
                
                if link and link not in unique_items:
                    name_el = await card.query_selector(".search-business-snippet-view__title")
                    name = await name_el.inner_text() if name_el else "Без названия"
                    addr_el = await card.query_selector(".search-business-snippet-view__address")
                    address = await addr_el.inner_text() if addr_el else ""
                    
                    unique_items[link] = {
                        "name": name.strip(),
                        "address": address.strip(),
                        "link": link,
                        "phone": ""
                    }
            except: continue

        curr = len(unique_items)
        bar.progress((i+1)/max_scrolls, text=f"Найдено: {curr}")
        
        if curr == last_len and curr > 0:
            stuck_counter += 1
            if stuck_counter >= 5: break
        else: stuck_counter = 0
        last_len = curr

        try:
            await page.hover(list_selector)
            await page.keyboard.press("PageDown")
            if i % 5 == 0: await page.keyboard.press("End")
            if cards: await cards[-1].scroll_into_view_if_needed()
        except: pass
        await asyncio.sleep(1.0)

    bar.empty()
    await page.close()
    return list(unique_items.values())


async def fetch_phone(context, item, semaphore):
    async with semaphore:
        page = await context.new_page()
        try:
            await asyncio.sleep(random.uniform(1.5, 4.0))
            await page.goto(item['link'], timeout=45000) # Увеличили таймаут для прокси

            try:
                await page.wait_for_selector(".orgpage-phones-view__phone-number", timeout=6000)
                els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                phones = [await e.inner_text() for e in els]
                item['phone'] = ", ".join(phones)
            except:
                item['phone'] = "Скрыт/Нет"
        except:
            item['phone'] = "Ошибка загрузки"
        finally:
            await page.close()

async def run_process(query):
    status = st.status("Запуск браузера с ПРОКСИ...", expanded=True)
    
    # 1. Получаем конфиг прокси
    proxy_conf = get_proxy_config()
    
    if not proxy_conf:
        status.error("❌ Прокси не настроены! Добавьте их в Secrets.")
        return None

    try:
        async with async_playwright() as p:
            # 2. Передаем прокси в браузер
            browser = await p.chromium.launch(
                headless=True, 
                proxy=proxy_conf 
            )
            
            # ignore_https_errors важен для прокси
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True 
            )
            
            # Этап 1
            items = await scrape_listing(context, query, status)
            
            if not items:
                status.error("Ничего не найдено.")
                return None
            
            if TEST_LIMIT_2:
                items = items[:2]
                status.warning("Тестовый режим: берем 2 шт.")
            
            status.write(f"Найдено {len(items)}. Сбор телефонов...")
            
            # Этап 2
            sem = asyncio.Semaphore(3) # 3 потока с прокси - безопасно
            tasks = [fetch_phone(context, item, sem) for item in items]
            
            ph_bar = st.progress(0, text="Обзвон...")
            for i, future in enumerate(asyncio.as_completed(tasks)):
                await future
                ph_bar.progress((i+1)/len(items))
            
            ph_bar.empty()
            status.update(label="Готово!", state="complete", expanded=False)
            return items

    except Exception as e:
        st.error("Критическая ошибка:")
        st.code(traceback.format_exc())
        return None

# --- ИНТЕРФЕЙС ---

if 'results' not in st.session_state:
    st.session_state.results = None

with st.sidebar:
    st.header("Настройки")
    
    if "proxy" in st.secrets:
        st.success("✅ Прокси подключены")
    else:
        st.error("❌ Нет настроек в Secrets")
    
    query = st.text_input("Запрос", value="Салон красоты Ташкент Юнусабад")
    
    if st.button("🚀 ЗАПУСТИТЬ"):
        st.session_state.results = asyncio.run(run_process(query))

if st.session_state.results:
    df = pd.DataFrame(st.session_state.results)
    st.dataframe(df)
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "proxy_data.csv")

