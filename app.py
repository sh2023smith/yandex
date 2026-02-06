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

st.set_page_config(page_title="Auto-Proxy Parser", page_icon="🔄", layout="wide")
st.title("🔄 Парсер (Ротация на каждый запрос)")

# --- ФУНКЦИИ ---

def get_proxy_config():
    if "proxy" in st.secrets:
        return {
            "server": f"http://{st.secrets['proxy']['server']}",
            "username": st.secrets['proxy']['username'],
            "password": st.secrets['proxy']['password']
        }
    return None

# --- ПАРСИНГ ---

async def scrape_listing(p, query, status_log, proxy_conf):
    # ВАЖНО: В режиме "На каждый запрос" мы должны создавать НОВЫЙ контекст
    # при каждой попытке, чтобы получить новый IP.
    
    unique_items = {}
    
    # Попытки входа (до 5 раз, так как IP меняется сам)
    for attempt in range(1, 6):
        browser = None
        try:
            # Запускаем браузер заново для смены IP
            browser = await p.chromium.launch(
                headless=True, 
                proxy=proxy_conf,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                ignore_https_errors=True
            )
            
            # Блокировка картинок (Экономия)
            await context.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "media", "font"] 
                else route.continue_()
            )

            page = await context.new_page()
            
            status_log.info(f"🔄 Попытка {attempt}: Заход с новым IP...")
            
            try:
                await page.goto("https://yandex.ru/maps", timeout=45000)
            except:
                status_log.warning(f"Таймаут (IP плохой). Пробую следующий...")
                await browser.close()
                continue

            # Проверка на капчу
            if await page.query_selector(".SmartCaptcha-Button") or \
               await page.query_selector("text=Подтвердите, что"):
                status_log.warning(f"🛑 Капча. Этот IP занят. Перезапуск...")
                await browser.close()
                continue # Просто перезапускаем цикл -> новый IP

            # Если прошли, ищем поиск
            try:
                await page.wait_for_selector("input.input__control", state="visible", timeout=15000)
                status_log.success(f"✅ Успешный вход!")
            except:
                status_log.warning("Поля нет. Меняем IP...")
                await browser.close()
                continue
            
            # --- ПОИСК ---
            await page.fill("input.input__control", query)
            await asyncio.sleep(1)
            await page.keyboard.press("Enter")
            
            status_log.info("⏳ Сбор списка...")
            list_selector = ".search-list-view__list"
            await page.wait_for_selector(list_selector, timeout=40000)
            await page.click(list_selector)
            
            # Скроллинг
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
                except: pass
                await asyncio.sleep(0.5)

            bar.empty()
            await browser.close()
            return list(unique_items.values())

        except Exception as e:
            if browser: await browser.close()
            status_log.error(f"Ошибка попытки: {e}")
            continue # Пробуем следующую попытку
            
    return []

async def fetch_phone(p, item, semaphore, proxy_conf):
    async with semaphore:
        # Для каждого телефона открываем НОВЫЙ браузер = НОВЫЙ IP
        # Это медленнее, но надежнее с вашим типом ротации
        browser = await p.chromium.launch(
            headless=True, 
            proxy=proxy_conf,
            args=["--disable-blink-features=AutomationControlled"]
        )
        try:
            context = await browser.new_context(ignore_https_errors=True)
            # Блокируем картинки для экономии
            await context.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "media", "font"] else route.continue_()
            )
            page = await context.new_page()
            
            try:
                await page.goto(item['link'], timeout=40000)
                
                # Если капча - считаем, что телефона нет (чтобы не висеть вечно)
                if await page.query_selector(".SmartCaptcha-Button"):
                    item['phone'] = "Капча (Skip)"
                else:
                    # Ищем кнопку
                    try:
                        btn = await page.query_selector("button:has-text('Показать телефон')") or \
                              await page.query_selector(".card-phones-view__more-button")
                        if btn: await btn.click()
                    except: pass
                    
                    # Собираем
                    phones = []
                    links = await page.query_selector_all("a[href^='tel:']")
                    for l in links:
                        h = await l.get_attribute("href")
                        if h: phones.append(h.replace("tel:", "").strip())
                    
                    if not phones:
                        els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                        for e in els: phones.append(await e.inner_text())
                    
                    item['phone'] = ", ".join(list(set(phones))) if phones else "Нет номера"
            except:
                item['phone'] = "Ошибка"
        finally:
            await browser.close()

async def run_process(query):
    status = st.status("🚀 Старт (Режим: Every Request)...", expanded=True)
    proxy_conf = get_proxy_config()
    
    if not proxy_conf:
        status.error("Нет настроек в Secrets!")
        return None

    async with async_playwright() as p:
        # Этап 1: Список
        items = await scrape_listing(p, query, status, proxy_conf)
        
        if not items:
            status.error("Не удалось собрать список после 5 попыток.")
            return None

        if TEST_LIMIT_2: items = items[:2]
        
        status.write(f"Найдено {len(items)}. Сбор телефонов...")
        
        # Этап 2: Телефоны
        # С ротацией "на каждый запрос" можно ставить больше потоков (5-6)
        sem = asyncio.Semaphore(5) 
        tasks = [fetch_phone(p, item, sem, proxy_conf) for item in items]
        
        ph_bar = st.progress(0, text="Обзвон...")
        for i, future in enumerate(asyncio.as_completed(tasks)):
            await future
            ph_bar.progress((i+1)/len(items))
        
        ph_bar.empty()
        status.update(label="✅ Готово!", state="complete", expanded=False)
        return items

# --- UI ---
if 'results' not in st.session_state: st.session_state.results = None

with st.sidebar:
    st.header("Настройки")
    if "proxy" in st.secrets: st.success("✅ Прокси активны")
    query = st.text_input("Запрос", value="Салон красоты Ташкент Юнусабад")
    if st.button("🚀 ПОЕХАЛИ", type="primary"):
        st.session_state.results = asyncio.run(run_process(query))

if st.session_state.results:
    df = pd.DataFrame(st.session_state.results)
    st.dataframe(df)
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "results.csv")
