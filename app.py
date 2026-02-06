import streamlit as st
import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
import sys
import subprocess
import traceback # Нужно для отлова ошибок

# --- 1. НАСТРОЙКИ ---
# Если True, скрипт возьмет только первые 2 записи для теста
TEST_LIMIT_2 = True 

# Фикс для Windows (на всякий случай)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

nest_asyncio.apply()

# --- 2. УСТАНОВКА БРАУЗЕРА ---
@st.cache_resource
def install_browser():
    # Эта функция выполняется один раз при старте сервера
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    except Exception as e:
        print(f"Error installing browser: {e}")

install_browser()

# --- 3. ИНТЕРФЕЙС ---
st.set_page_config(page_title="Yandex Debugger", page_icon="🐞", layout="wide")
st.title("🐞 Парсер (Режим отладки: 2 ссылки)")

if 'results' not in st.session_state:
    st.session_state.results = None

with st.sidebar:
    st.header("Настройки")
    if st.button("🔴 СБРОСИТЬ ВСЁ", type="primary"):
        st.session_state.results = None
        st.rerun()
    
    st.divider()
    search_query = st.text_input("Запрос", value="Салон красоты Ташкент Юнусабад")
    st.info("Сейчас включен лимит: обработка только 2-х карточек для теста.")

# --- 4. ФУНКЦИИ ПАРСИНГА ---

async def scrape_listing(context, query, status_log):
    """Этап 1: Сбор ссылок из левой колонки"""
    page = await context.new_page()
    status_log.write(f"🔍 Ищу: {query}")
    
    try:
        await page.goto("https://yandex.ru/maps", timeout=40000)
        await page.wait_for_selector("input.input__control", timeout=20000)
        await page.fill("input.input__control", query)
        await page.keyboard.press("Enter")
        
        list_selector = ".search-list-view__list"
        await page.wait_for_selector(list_selector, timeout=20000)
        await page.click(list_selector)
    except Exception as e:
        status_log.error(f"Ошибка поиска: {e}")
        return []

    unique_items = {}
    stuck_counter = 0
    last_len = 0
    
    # Скроллим немного, нам много не надо для теста
    max_scrolls = 15 
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
            if stuck_counter >= 3: break
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

async def fetch_phone_debug(context, item, semaphore):
    """Этап 2: Заход в карточку + СКРИНШОТ при ошибке"""
    async with semaphore:
        page = await context.new_page()
        screenshot = None
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            # Таймаут 25 сек
            await page.goto(item['link'], timeout=25000)
            
            try:
                # Ждем телефон
                await page.wait_for_selector(".orgpage-phones-view__phone-number", timeout=5000)
                els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                phones = [await e.inner_text() for e in els]
                item['phone'] = ", ".join(phones)
            except:
                item['phone'] = "Нет/Скрыт (см. скрин)"
                # ДЕЛАЕМ СКРИНШОТ, ЕСЛИ ТЕЛЕФОНА НЕТ
                screenshot = await page.screenshot(full_page=False)

        except Exception as e:
            item['phone'] = f"Ошибка: {str(e)}"
        finally:
            await page.close()
            return screenshot

async def main_logic():
    status = st.status("Запуск браузера...", expanded=True)
    browser = None
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            )
            
            # 1. Список
            items = await scrape_listing(context, search_query, status)
            
            if not items:
                status.error("Список пуст. Возможно капча сразу на входе.")
                return None

            # --- ОГРАНИЧЕНИЕ В 2 ССЫЛКИ ---
            if TEST_LIMIT_2:
                status.warning(f"Найдено {len(items)}, но берем только 2 для теста!")
                items = items[:2]
            else:
                status.write(f"Найдено {len(items)}. Обрабатываем все...")
            
            # 2. Телефоны
            sem = asyncio.Semaphore(1) # Строго 1 поток для стабильности
            
            # Обертка для задач
            async def task_wrapper(ctx, itm, sm):
                return await fetch_phone_debug(ctx, itm, sm)

            tasks = [task_wrapper(context, item, sem) for item in items]
            
            ph_bar = st.progress(0, text="Заход в карточки...")
            
            debug_expander = st.expander("📸 Скриншоты (Что видит бот)", expanded=True)
            
            for i, future in enumerate(asyncio.as_completed(tasks)):
                screenshot = await future
                
                # Если вернулся скриншот - показываем
                if screenshot:
                    with debug_expander:
                        st.image(screenshot, caption=f"Скриншот {i+1}", use_container_width=True)
                
                ph_bar.progress((i+1)/len(items))
            
            ph_bar.empty()
            status.update(label="Готово!", state="complete", expanded=False)
            return items

    except Exception as e:
        # ВОТ ЭТО ПОКАЖЕТ ОШИБКУ НА ЭКРАНЕ ВМЕСТО ВЫЛЕТА
        st.error("💥 Произошла критическая ошибка!")
        st.code(traceback.format_exc())
        return None

# --- ЗАПУСК ПО КНОПКЕ ---
if st.session_state.results is None:
    if st.button("🚀 НАЧАТЬ ТЕСТ (2 ссылки)", type="primary"):
        # Запускаем через asyncio.run, оборачивая в try-except на верхнем уровне
        try:
            st.session_state.results = asyncio.run(main_logic())
            st.rerun()
        except Exception as e:
            st.error("Ошибка запуска Asyncio:")
            st.code(traceback.format_exc())

else:
    st.success("Обработка завершена")
    df = pd.DataFrame(st.session_state.results)
    st.dataframe(df)
    
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "debug_data.csv", "text/csv")
    
    if st.button("🔄 Новый поиск"):
        st.session_state.results = None
        st.rerun()
