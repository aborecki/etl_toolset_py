chrome_driver=r"C:\tools\seleniumchromedriver\chromedriver.exe"

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


browser = webdriver.Chrome(executable_path=chrome_driver)
browser.get('https://bec.operations.dynamics.com/?mi=TSTimesheetEntryGridViewMyTimesheets')
assert "Sign in" in browser.title
elem = browser.find_element_by_name("loginfmt")
elem.clear()
elem.send_keys("zva@bec.dk")
elem.send_keys(Keys.RETURN)
assert "Moje karty czasu pracy" in browser.title