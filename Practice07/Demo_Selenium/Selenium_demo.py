import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
def get_page(driver, url):
    driver.get(url)
    products = driver.find_elements(By.CLASS_NAME, "product-item")
    sleep(0.5)
    title_list = [i.text for i in driver.find_elements(By.XPATH, '//div [@class ="name"]//child::h3')]
    price_list = [i.text for i in driver.find_elements(By.CLASS_NAME, 'price-discount__price')]
    return [price_list, title_list]
def main():
    driver = webdriver.Edge("Source/msedgedriver")
    #driver = webdriver.Chrome('Source/chromedriver')


    driver.maximize_window()
    driver.implicitly_wait(10) # Thời gian để driver chờ đợi các phần tử được load
    lst_price = []
    lst_title = []
    i=1
    while True:
        try:
            url = "https://tiki.vn/sach-truyen-tieng-viet/c316?page=" + str(i)
            data = get_page(driver, url)
            if data[0] == []:
                break
            lst_price.extend(data[0])
            lst_title.extend(data[1])
            i+=1
        except:
            break
    data = {"title": lst_title, "price":lst_price}
    df = pd.DataFrame(data)
    print(df)
    #df.to_excel("Tiki3.xlsx", engine="xlsxwriter", encoding='utf-8', index=False)
    file_name = "Data_from_Tiki.csv"
    df.to_csv(file_name, encoding='utf-8', index=False)
    driver.close()
if __name__ == "__main__":
    main() 


