import streamlit as st
import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
import sys
import subprocess
import os

# --- 1. НАСТРОЙКИ ДЛЯ ОБЛАКА И WINDOWS ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

nest_asyncio.apply()


# --- 2. АВТО-УСТАНОВКА БРАУЗЕРА (ДЛЯ CLOUD) ---
# Streamlit Cloud каждый раз создает чистый контейнер, поэтому браузер нужно качать заново.
@st.cache_resource
def install_playwright_browser():
    try:
        # Проверяем, установлен ли браузер, запуская простую команду
        # Если это первый запуск, скачиваем chromium
        print("Installing Playwright Chromium...")
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        print("Browser installed!")
    except Exception as e:
        print(f"Error installing browser: {e}")


# Запускаем установку 1 раз при старте приложения
install_playwright_browser()

# --- 3. ИНТЕРФЕЙС ---
st.set_page_config(page_title="Yandex Maps Parser", page_icon="🗺️", layout="wide")
st.title("🗺️ Парсер Яндекс.Карт (Web Version)")

# Инициализация сессии
if 'results' not in st.session_state:
    st.session_state.results = None

with st.sidebar:
    st.header("Настройки")
    if st.button("🔄 Сброс (Новый поиск)", type="secondary"):
        st.session_state.results = None
        st.rerun()
    st.divider()
    search_query = st.text_input("Поисковый запрос", value="Кофейня Ташкент Центр")
    # В облаке лучше ограничить потоки
    concurrency = st.slider("Потоки", 1, 3, 1)
    st.info("ℹ️ В бесплатном облаке IP-адреса серверные. Яндекс может быстро выдать капчу.")


# --- ЛОГИКА ---
async def scrape_listing(context, query, status_log):
    page = await context.new_page()
    status_log.write(f"🔍 [1/2] Поиск: {query}")

    try:
        await page.goto("https://yandex.ru/maps", timeout=60000)
        await page.wait_for_selector("input.input__control", timeout=20000)
        await page.fill("input.input__control", query)
        await page.keyboard.press("Enter")

        list_selector = ".search-list-view__list"
        await page.wait_for_selector(list_selector, timeout=20000)
        await page.click(list_selector)
    except Exception as e:
        status_log.error(f"Ошибка (возможно капча): {e}")
        return []

    unique_items = {}
    stuck_counter = 0
    last_len = 0

    my_bar = st.progress(0, text="Скроллинг...")
    max_scrolls = 30  # Уменьшил для стабильности в облаке

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

                    unique_items[link] = {"name": name.strip(), "address": address.strip(), "link": link, "phone": ""}
            except:
                continue

        curr = len(unique_items)
        my_bar.progress((i + 1) / max_scrolls, text=f"Шаг {i + 1}/{max_scrolls}. Найдено: {curr}")

        if curr == last_len and curr > 0:
            stuck_counter += 1
            if stuck_counter >= 4: break
        else:
            stuck_counter = 0
        last_len = curr

        try:
            await page.hover(list_selector)
            await page.keyboard.press("PageDown")
            if i % 5 == 0: await page.keyboard.press("End")
            if cards: await cards[-1].scroll_into_view_if_needed()
        except:
            pass
        await asyncio.sleep(1.0)

    my_bar.empty()
    await page.close()
    return list(unique_items.values())


async def fetch_phone(context, item, semaphore):
    async with semaphore:
        page = await context.new_page()
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await page.goto(item['link'], timeout=40000)
            try:
                await page.wait_for_selector(".orgpage-phones-view__phone-number", timeout=5000)
                els = await page.query_selector_all(".orgpage-phones-view__phone-number")
                item['phone'] = ", ".join([await e.inner_text() for e in els])
            except:
                item['phone'] = "-"
        except:
            item['phone'] = "Ошибка"
        finally:
            await page.close()


async def run_process():
    status = st.status("Запуск...", expanded=True)
    async with async_playwright() as p:
        # В облаке ВСЕГДА headless=True
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})

        items = await scrape_listing(context, search_query, status)
        if not items:
            status.error("Пусто.")
            return None

        status.write(f"Найдено {len(items)}. Сбор телефонов...")
        sem = asyncio.Semaphore(concurrency)
        tasks = [fetch_phone(context, item, sem) for item in items]
        await asyncio.gather(*tasks)

        status.update(label="Готово!", state="complete", expanded=False)
        return items


if st.session_state.results is None:
    if st.button("🚀 Начать", type="primary"):
        st.session_state.results = asyncio.run(run_process())
        st.rerun()
else:
    df = pd.DataFrame(st.session_state.results)
    st.success(f"Собрано: {len(df)}")
    st.dataframe(df)
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("Скачать CSV", csv, "data.csv", "text/csv")