from selenium import webdriver
#driver = webdriver.Chrome()
#driver = webdriver.Chrome('Source/chromedriver')
driver = webdriver.Edge("Source/msedgedriver")
url ='https://webtrading.ssi.com.vn/Logon?lang=en-us'
driver.get(url)
e_name = 'txtname'
e_pw = 'txt_password'
e_Login = 'btlogin'
e_sell = 'btOrderSell'
driver.find_element_by_id(e_name).send_keys('307594')
driver.find_element_by_id(e_pw).send_keys('Gp*JQ@ne7&1Bc')
driver.find_element_by_id(e_Login).click()
driver.implicitly_wait(2)
driver.find_element_by_id('spanSell').click()
driver.find_element_by_id('txtStockSymbol').send_keys('HAG')
driver.find_element_by_id('txtOrderUnits').send_keys('100')
driver.find_element_by_id('txtOrderPrice').send_keys('25')
driver.find_element_by_id('txtSecureCode').send_keys('KpOJQQne4321qR')
driver.find_element_by_id('btnOrder').click()
driver.find_element_by_id('btnOrder').click()
driver.quit()















