import streamlit as st
import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
import sys
import subprocess
import traceback
import urllib.request
import ssl

# --- НАСТРОЙКИ ---
TEST_LIMIT_2 = False 

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

nest_asyncio.apply()

@st.cache_resource
def install_browser():
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    except Exception as e:
        print(f"Error installing browser: {e}")

install_browser()

st.set_page_config(page_title="Universal Parser", page_icon="📞", layout="wide")
st.title("📞 Парсер: Универсальный сборщик")

# --- ФУНКЦИИ ПРОКСИ ---

def get_proxy_config():
    if "proxy" in st.secrets:
        return {
            "server": f"http://{st.secrets['proxy']['server']}",
            "username": st.secrets['proxy']['username'],
            "password": st.secrets['proxy']['password'],
            "api_url": st.secrets['proxy'].get('api_url')
        }
    return None

def rotate_ip(api_url):
    if not api_url: return False
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(api_url, context=ctx, timeout=10) as response:
            return response.status == 200
    except:
        return False

# --- ПАРСИНГ ---

async def scrape_listing(context, query, status_log, proxy_conf):
    page = await context.new_page()
    status_log.info(f"🔍 Вход в список...")

    # Попытки входа (ротация)
    for attempt in range(1, 4):
        try:
            try:
                await page.goto("https://yandex.ru/maps", timeout=60000)
            except: pass
            
            # Проверка капчи
            is_captcha = await page.query_selector(".SmartCaptcha-Button") or \
                         await page.query_selector("text=Подтвердите, что")

            if is_captcha:
                if proxy_conf and proxy_conf.get('api_url'):
                    status_log.warning(f"🛑 Капча на входе. Ротация IP (Попытка {attempt})...")
                    rotate_ip(proxy_conf['api_url'])
                    await asyncio.sleep(15)
                    await context.clear_cookies()
                    continue 
                else:
                    return []

            try:
                await page.wait_for_selector("input.input__control", state="visible", timeout=20000)
                status_log.success(f"✅ Вход выполнен.")
                break 
            except:
                status_log.warning("Поля поиска нет. Ротация...")
                if proxy_conf and proxy_conf.get('api_url'):
                    rotate_ip(proxy_conf['api_url'])
                    await asyncio.sleep(15)
                    await context.clear_cookies()
                    continue
        except Exception as e:
            return []
    
    # Поиск
    try:
        await page.fill("input.input__control", query)
        await asyncio.sleep(1)
        await page.keyboard.press("Enter")
        
        list_selector = ".search-list-view__list"
        await page.wait_for_selector(list_selector, timeout=45000)
        await page.click(list_selector)
    except:
        status_log.error("❌ Не удалось загрузить список.")
        return []

    unique_items = {}
    stuck_counter = 0
    last_len = 0
    max_scrolls = 30 
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
                    unique_items[link] = {"name": name.strip(), "link": link, "phone": ""}
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
            if cards: await cards[-1].scroll_into_view_if_needed()
        except: pass
        await asyncio.sleep(1.0)

    bar.empty()
    await page.close()
    return list(unique_items.values())

# --- УЛУЧШЕННАЯ ФУНКЦИЯ ПОИСКА ТЕЛЕФОНА ---
async def fetch_phone(context, item, semaphore):
    async with semaphore:
        page = await context.new_page()
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            # Таймаут побольше для прокси
            await page.goto(item['link'], timeout=50000)

            # 1. Проверка на капчу внутри карточки
            if await page.query_selector(".SmartCaptcha-Button"):
                item['phone'] = "Капча (Пропуск)"
                return

            # 2. Ищем кнопку "Показать телефон" и кликаем
            try:
                # Ищем кнопку по тексту или классу
                show_btn = await page.query_selector("button:has-text('Показать телефон')") or \
                           await page.query_selector(".card-phones-view__more-button")
                if show_btn:
                    await show_btn.click()
                    await asyncio.sleep(1) # Ждем раскрытия
            except: pass

            # 3. СТРАТЕГИЯ "ПЫЛЕСОС": Собираем все ссылки tel:
            # Это самый надежный способ, работает на мобилках и десктопе
            phones_found = []
            
            # Ищем теги <a href="tel:...">
            tel_links = await page.query_selector_all("a[href^='tel:']")
            for link in tel_links:
                href = await link.get_attribute("href")
                if href:
                    # Убираем 'tel:' и добавляем в список
                    clean_phone = href.replace("tel:", "").strip()
                    if clean_phone not in phones_found:
                        phones_found.append(clean_phone)
            
            # 4. Если ссылок нет, пробуем искать старый добрый текст
            if not phones_found:
                els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                for e in els:
                    txt = await e.inner_text()
                    if txt not in phones_found:
                        phones_found.append(txt)

            if phones_found:
                item['phone'] = ", ".join(phones_found)
            else:
                item['phone'] = "Нет номера"

        except Exception as e:
            item['phone'] = "Ошибка загрузки"
        finally:
            await page.close()

async def run_process(query):
    status = st.status("🚀 Инициализация...", expanded=True)
    proxy_conf = get_proxy_config()
    
    if not proxy_conf:
        status.error("Нет настроек в Secrets!")
        return None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy=proxy_conf)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                # Маскируемся под обычный Chrome
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                ignore_https_errors=True
            )
            
            items = await scrape_listing(context, query, status, proxy_conf)
            
            if not items:
                status.error("Список пуст.")
                return None

            if TEST_LIMIT_2: items = items[:2]
            
            status.write(f"Найдено {len(items)}. Сбор телефонов (Медленный режим)...")
            
            # ВАЖНО: Снижаем до 2 потоков, чтобы прокси успевал грузить страницы
            sem = asyncio.Semaphore(2) 
            tasks = [fetch_phone(context, item, sem) for item in items]
            
            ph_bar = st.progress(0, text="Обзвон...")
            for i, future in enumerate(asyncio.as_completed(tasks)):
                await future
                ph_bar.progress((i+1)/len(items))
            
            ph_bar.empty()
            status.update(label="✅ Готово!", state="complete", expanded=False)
            return items

    except Exception as e:
        st.error(f"Критическая ошибка: {e}")
        st.code(traceback.format_exc())
        return None

# --- UI ---
if 'results' not in st.session_state: st.session_state.results = None

with st.sidebar:
    st.header("Панель управления")
    if "proxy" in st.secrets:
        st.success("✅ Прокси OK")
        
    query = st.text_input("Запрос", value="Салон красоты Ташкент Юнусабад")
    
    if st.button("🚀 ПОЕХАЛИ", type="primary"):
        st.session_state.results = asyncio.run(run_process(query))

if st.session_state.results:
    df = pd.DataFrame(st.session_state.results)
    st.dataframe(df)
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "results.csv")
