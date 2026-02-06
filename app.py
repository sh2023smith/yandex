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

st.set_page_config(page_title="Debug Parser", page_icon="📸", layout="wide")
st.title("📸 Парсер с Фото-Отчетом ошибок")

# --- ФУНКЦИИ ---

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

async def scrape_listing(context, query, status_log, proxy_conf):
    page = await context.new_page()
    status_log.info(f"🔍 Захожу на Яндекс...")

    # --- 1. ВХОД НА САЙТ (С РОТАЦИЕЙ) ---
    for attempt in range(1, 4):
        try:
            try:
                await page.goto("https://yandex.ru/maps", timeout=60000)
            except:
                status_log.warning(f"⚠️ Таймаут загрузки страницы (Попытка {attempt}).")
            
            # Проверка на капчу
            is_captcha = await page.query_selector(".SmartCaptcha-Button") or \
                         await page.query_selector("text=Подтвердите, что")

            if is_captcha:
                if proxy_conf and proxy_conf.get('api_url'):
                    status_log.warning(f"🛑 Капча. Меняю IP (Попытка {attempt}/3)... Ждите 15 сек.")
                    rotate_ip(proxy_conf['api_url'])
                    await asyncio.sleep(15)
                    await context.clear_cookies()
                    continue 
                else:
                    return []

            # Ищем поле поиска
            try:
                await page.wait_for_selector("input.input__control", state="visible", timeout=20000)
                status_log.success(f"✅ Успешный вход (Попытка {attempt})!")
                break 
            except:
                status_log.warning("Поле поиска не появилось. Меняю IP...")
                if proxy_conf and proxy_conf.get('api_url'):
                    rotate_ip(proxy_conf['api_url'])
                    await asyncio.sleep(15)
                    await context.clear_cookies()
                    continue
                
        except Exception as e:
            status_log.error(f"Сбой: {e}")
            return []
    
    # --- 2. ПОИСК И СПИСОК ---
    try:
        # Вводим запрос
        await page.fill("input.input__control", query)
        await asyncio.sleep(1) # Пауза перед Enter
        await page.keyboard.press("Enter")
        
        status_log.info("⏳ Запрос отправлен. Жду список результатов (45 сек)...")
        
        list_selector = ".search-list-view__list"
        
        try:
            # Ждем список 45 секунд
            await page.wait_for_selector(list_selector, timeout=45000)
            await page.click(list_selector)
        except:
            # !!! ЕСЛИ НЕ НАШЛИ СПИСОК - ФОТОГРАФИРУЕМ !!!
            status_log.error("❌ Список не появился вовремя.")
            screenshot = await page.screenshot()
            st.image(screenshot, caption="ЭКРАН ОШИБКИ: Что видит бот?", width=600)
            status_log.warning("☝️ Посмотрите на фото выше. Если там пусто - Яндекс тормозит. Если там 'Ничего не найдено' - проверьте запрос.")
            return []

    except Exception as e:
        status_log.error(f"Ошибка логики поиска: {e}")
        return []

    # --- 3. СБОР ССЫЛОК ---
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

async def fetch_phone(context, item, semaphore):
    async with semaphore:
        page = await context.new_page()
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await page.goto(item['link'], timeout=45000)
            try:
                await page.wait_for_selector(".orgpage-phones-view__phone-number", timeout=5000)
                els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                phones = [await e.inner_text() for e in els]
                item['phone'] = ", ".join(phones)
            except:
                item['phone'] = "Скрыт/Нет"
        except:
            item['phone'] = "Ошибка"
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
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                ignore_https_errors=True
            )
            
            items = await scrape_listing(context, query, status, proxy_conf)
            
            if not items:
                status.error("Список пуст.")
                return None

            if TEST_LIMIT_2: items = items[:2]
            
            status.write(f"Найдено {len(items)}. Сбор телефонов...")
            
            sem = asyncio.Semaphore(5)
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
    if "proxy" in st.secrets and "api_url" in st.secrets["proxy"]:
        st.success("✅ Авто-смена IP подключена")
    else:
        st.error("❌ Нет API URL в Secrets")
        
    query = st.text_input("Запрос", value="Салон красоты Ташкент Юнусабад")
    
    if st.button("🚀 ПОЕХАЛИ", type="primary"):
        st.session_state.results = asyncio.run(run_process(query))

if st.session_state.results:
    df = pd.DataFrame(st.session_state.results)
    st.dataframe(df)
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "results.csv")
