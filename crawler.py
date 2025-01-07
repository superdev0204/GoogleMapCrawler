from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import os
import mysql.connector
from mysql.connector import Error
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains, Keys
from selenium.webdriver.common.keys import Keys
import pandas as pd
import requests_html
import re
import csv
from selenium.webdriver.support.ui import WebDriverWait


class GoogleMaps:
    def __init__(self):
        options = webdriver.ChromeOptions()
        print("Crawler Logs: Starting the crawler....")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-application-cache')
        options.add_argument('--disable-gpu')
        options.add_argument("--disable-dev-shm-usage")
        # options.add_argument("--headless=new")
        self.driver = webdriver.Chrome(options=options)
        self.actionChains = ActionChains(self.driver)
        self.wait = WebDriverWait(self.driver, 20)
        self.driver.get("https://www.google.com/search?q=flooring+companies+denver%2C+co&sca_esv=658a7a69cf297c41&hl=en&authuser=0&biw=1920&bih=931&tbm=lcl&sxsrf=ACQVn09bM2-Rtuq7SewvZNm_PNA3tOBVXA%3A1706613001199&ei=Cdm4ZafnC4bAxc8P-JaK-AI&ved=0ahUKEwjnj9Lt_ISEAxUGYPEDHXiLAi8Q4dUDCAk&uact=5&oq=flooring+companies+denver%2C+co&gs_lp=Eg1nd3Mtd2l6LWxvY2FsIh1mbG9vcmluZyBjb21wYW5pZXMgZGVudmVyLCBjbzIEECMYJzIFEAAYgAQyCxAAGIAEGIoFGIYDMgsQABiABBiKBRiGA0j9HVCJHFiJHHACeACQAQCYAYgCoAGPBaoBBTAuMS4yuAEDyAEA-AEBiAYB&sclient=gws-wiz-local#rlfi=hd:;si:;start:20;tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3!3sIAE,lf:1,lf_ui:10")
        language_code = "en-US"
        self.driver.execute_script(f"document.documentElement.lang = '{language_code}';")
        self.driver.refresh()
        
        self.file_path = "Scrapper Keywords.xlsx"
        self.table_name = 'company_infos_google'
        self.cnx = None
        self.cursor = None

        self.start_keyword=''
        self.start_city=''
        self.start_page=1
        # self.start_company=0
        if os.path.exists("./scraping_progress.csv"):
            with open('scraping_progress.csv', 'r') as csvfile_progress:
                reader_progress = csv.reader(csvfile_progress)
                for row_progress in reader_progress:
                    try:
                        value = row_progress[0]
                        self.start_keyword = str(value)
                    except IndexError:
                        self.start_keyword = ''
                    
                    try:
                        value = row_progress[1]
                        self.start_city = str(value)
                    except IndexError:
                        self.start_city = ''

                    try:
                        value = row_progress[2]
                        self.start_page = int(value)
                    except IndexError:
                        self.start_page = 1
                    
                csvfile_progress.close()

        else:
            self.start_keyword=''
            self.start_city=''
            self.start_page=1
            # self.start_company=0
        
        try:
            self.cnx = mysql.connector.connect(
                    host="103.35.191.223",
                    user="ah123",
                    password="ah123",
                    database="crawler"
                )
            if self.cnx.is_connected():
                print('Connected to MySQL database')

        except Error as e:
            try:
                self.cnx = mysql.connector.connect(
                        host="192.168.0.97",
                        user="ah123",
                        password="ah123",
                        database="crawler"
                    )
                if self.cnx.is_connected():
                    print('Connected to MySQL database')
            except Error as e:
                print('Error connecting to MySQL database:', e)

        self.cursor = self.cnx.cursor()
        
        query = f"SELECT * FROM cities Order By id Asc"
        self.cursor.execute(query)
        cities = self.cursor.fetchall()
        self.usa_cities =  []
        for city in cities:
            self.usa_cities.append(city[1])

        self.template = " Companies in "
        
    def reconnect(self):
        try:
            # Close the cursor if it exists
            if self.cursor:
                self.cursor.close()
            # Close the connection if it exists
            if self.cnx:
                self.cnx.close()

            time.sleep(3)
            self.cnx = mysql.connector.connect(
                host="103.35.191.223",
                user="ah123",
                password="ah123",
                database="crawler"
            )
            self.cursor = self.cnx.cursor()
            print('Reconnected to MySQL database')
        except Error as e:
            print('Error reconnecting to MySQL database:', e)
    
    def load_keywords(self):
        return [
            ['', 'Fence Installation'],
            ['', 'Flooring Contractors'],
            ['', 'Handyman'],
            ['', 'Drywall Repair'],
            ['', 'Carpentry'],
            ['', 'Faucet Installation'],
            ['', 'Faucet Repair'],
            ['', 'Fence Repair'],
            ['', 'Fire Damage Restoration'],
            ['', 'Heat Pump Installation'],
            ['', 'Heat Pump Repair']
        ]
        query = f"SELECT * FROM googlemap_keywords where keyword='Construction Services' Order By id Asc"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def check_words_in_string(self, input_string):
        cursor = self.cnx.cursor()
        query = f"SELECT * FROM exclude_keywords Order By id Asc"
        cursor.execute(query)
        words = cursor.fetchall()
        for word in words:
            if word[1] in input_string:
                return True
        return False

    def resolved_captcha(self, soup, url, sec):
        try:
            if sec > 12000:
                return
            captcha_element = soup.find(id="captcha-form")
            if captcha_element:
                print("Try to solve this captcha by manually,u have 10 sec")
                time.sleep(60)
                self.driver.get(url)
                resoup = BeautifulSoup(self.driver.page_source, "lxml")
                self.resolved_captcha(resoup, url, (sec + 1))
        except Exception:
            pass
    
    def google_map_crawler(self):
        try:
            time.sleep(1)
            keywords = self.load_keywords()
            
            self.cursor.execute(f"SHOW TABLES LIKE '{self.table_name}'")
            exists = self.cursor.fetchone()

            if exists:
                print(f"The table '{self.table_name}' exists.")

            else:
                create_table_query = f"CREATE TABLE {self.table_name} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), phone VARCHAR(255), location VARCHAR(255), profile TEXT)"
                try:
                    self.cursor.execute(create_table_query)
                except Error as e:
                    print('Error creating table:', e)
            
            keyword_flag = False
            city_flag = False

            if self.start_keyword == '':
                keyword_flag = True

            if self.start_city == '':
                city_flag = True
            
            for keyword_row in keywords:
                if(keyword_row[1] == self.start_keyword):
                    keyword_flag = True
                
                if(keyword_row[1] == self.start_keyword):
                    keyword_flag = True

                index = 0
                while index < len(self.usa_cities):
                    if keyword_flag == True and city_flag == True:
                        complete_keyword = keyword_row[1]+self.template+self.usa_cities[index]+ ', Usa'
                        print( f"Crawler Logs: Crawling Google Maps for keyword: {complete_keyword}." )

                        maxpage = 1
                        try:
                            url = f"https://www.google.com/search?q={complete_keyword}&sca_esv=658a7a69cf297c41&hl=en&authuser=0&biw=1920&bih=931&tbm=lcl&sxsrf=ACQVn09bM2-Rtuq7SewvZNm_PNA3tOBVXA%3A1706613001199&ei=Cdm4ZafnC4bAxc8P-JaK-AI&ved=0ahUKEwjnj9Lt_ISEAxUGYPEDHXiLAi8Q4dUDCAk&uact=5&oq=flooring+companies+denver%2C+co&gs_lp=Eg1nd3Mtd2l6LWxvY2FsIh1mbG9vcmluZyBjb21wYW5pZXMgZGVudmVyLCBjbzIEECMYJzIFEAAYgAQyCxAAGIAEGIoFGIYDMgsQABiABBiKBRiGA0j9HVCJHFiJHHACeACQAQCYAYgCoAGPBaoBBTAuMS4yuAEDyAEA-AEBiAYB&sclient=gws-wiz-local#rlfi=hd:;si:;tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3!3sIAE,lf:1,lf_ui:10"
                            self.driver.get(url)
                            time.sleep(3)

                            soup = BeautifulSoup(self.driver.page_source, "lxml")
                            self.resolved_captcha(soup, url, 0)

                            maxpage = self.driver.find_elements(By.XPATH, "//tr[@jsname='TeSSVd']/td")
                            try:
                                maxpage = maxpage[-2].find_element(By.XPATH, "./a").text
                            except:
                                maxpage = 1
                            
                            if self.start_page == 1:
                                init_page = 1
                            else:
                                init_page = self.start_page+1

                            for page_index in range(init_page, int(maxpage)+1):
                                start = (page_index - 1) * 20
                                url = f"https://www.google.com/search?q={complete_keyword}&sca_esv=658a7a69cf297c41&hl=en&authuser=0&biw=1920&bih=931&tbm=lcl&sxsrf=ACQVn09bM2-Rtuq7SewvZNm_PNA3tOBVXA%3A1706613001199&ei=Cdm4ZafnC4bAxc8P-JaK-AI&ved=0ahUKEwjnj9Lt_ISEAxUGYPEDHXiLAi8Q4dUDCAk&uact=5&oq=flooring+companies+denver%2C+co&gs_lp=Eg1nd3Mtd2l6LWxvY2FsIh1mbG9vcmluZyBjb21wYW5pZXMgZGVudmVyLCBjbzIEECMYJzIFEAAYgAQyCxAAGIAEGIoFGIYDMgsQABiABBiKBRiGA0j9HVCJHFiJHHACeACQAQCYAYgCoAGPBaoBBTAuMS4yuAEDyAEA-AEBiAYB&sclient=gws-wiz-local#rlfi=hd:;si:;start:{start};tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3!3sIAE,lf:1,lf_ui:10"
                                self.driver.get(url)
                                time.sleep(2)
                            
                                soup = BeautifulSoup(self.driver.page_source, "lxml")
                                self.resolved_captcha(soup, url, 0)
                                self.google_map_scrapper(self.driver.page_source, complete_keyword)

                                with open('scraping_progress.csv', 'w', newline='') as csvfile_progress:
                                    progress_writer = csv.writer(csvfile_progress)
                                    progress_writer.writerow([keyword_row[1], self.usa_cities[index], page_index])

                            self.start_page = 1

                        except Exception as e:
                            self.driver.get("https://www.google.com/search?q=flooring+companies+denver%2C+co&sca_esv=658a7a69cf297c41&hl=en&authuser=0&biw=1920&bih=931&tbm=lcl&sxsrf=ACQVn09bM2-Rtuq7SewvZNm_PNA3tOBVXA%3A1706613001199&ei=Cdm4ZafnC4bAxc8P-JaK-AI&ved=0ahUKEwjnj9Lt_ISEAxUGYPEDHXiLAi8Q4dUDCAk&uact=5&oq=flooring+companies+denver%2C+co&gs_lp=Eg1nd3Mtd2l6LWxvY2FsIh1mbG9vcmluZyBjb21wYW5pZXMgZGVudmVyLCBjbzIEECMYJzIFEAAYgAQyCxAAGIAEGIoFGIYDMgsQABiABBiKBRiGA0j9HVCJHFiJHHACeACQAQCYAYgCoAGPBaoBBTAuMS4yuAEDyAEA-AEBiAYB&sclient=gws-wiz-local#rlfi=hd:;si:;start:20;tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3!3sIAE,lf:1,lf_ui:10")
                            try:
                                if int(maxpage) == 1:
                                    index -= 1
                            except:
                                index -= 1
                                pass
                    else:
                        if(keyword_flag == True and self.usa_cities[index] == self.start_city):
                            city_flag = True
                        index += 1
                        continue
                        
                    index += 1
        finally:
            print("Crawler Logs: Srapping finished. Crawler is Stopping.")
            self.driver.quit()

    def google_map_scrapper(self, html, keyword):          
        print(f"Crawler Logs: Scrapping Google Maps for keyword: {keyword}.")
        soup = BeautifulSoup(html, "lxml")
        results = soup.find_all('div', 'rllt__details')
        if len(results) > 0:
            del results[0]
        # profiles = soup.find_all('a', 'hfpxzc')
        # init_company=1
        no = 0
        if len(results) > 0:
            for i in results: 
                # if init_company >= self.start_company:
                try:
                    name = i.find('span', 'OSrXXb').get_text()
                    name = name.replace('"', "'")
                    if self.check_words_in_string(name) == True:
                        print('wrong company info:' + name)
                        continue
                    profile = self.driver.current_url
                    # print(profile)
                    try:
                        phone = i.find_all('div')[3].get_text()                        
                        phone_strs = re.split('·',phone)
                        phone = phone_strs[len(phone_strs)-1]

                        location = i.find_all('div')[2].get_text()                        
                        location_strs = re.split('·',location)
                        try:
                            location = location_strs[1]
                        except:
                            location = ""

                        if phone.find("+1") == -1 and phone.find("(") == -1:
                            location = ""
                            phone = i.find_all('div')[2].get_text()                        
                            phone_strs = re.split('·',phone)
                            phone = phone_strs[len(phone_strs)-1]
                            if phone.find("+1") == -1 and phone.find("(") == -1:
                                location = ""
                                phone = i.find_all('div')[4].get_text()                        
                                phone_strs = re.split('·',phone)
                                phone = phone_strs[len(phone_strs)-1]
                                if phone.find("+1") == -1 and phone.find("(") == -1:
                                    phone = ""
                        
                        phone = phone.replace("'", "").replace('"', '').strip()
                        if phone.startswith("+1"):
                            # Remove the country code and any spaces or hyphens
                            phone = phone.replace("+1 ", "(").replace("-", ") ", 1)
                        location = location.replace('"', "'")
                    except:
                        phone = ''
                        location = ''
                 
                    # try:
                    #     inner_link = i.find('a', 'lcr4fd S9kvJb').get('href')
                    # except:
                    #     inner_link = ''
                    
                    if phone == '':
                        print('Wrong phone: '+name.strip())                        
                    else:
                        query = f'SELECT * FROM {self.table_name} WHERE phone = "{phone.strip()}"'
                        try:
                            self.cursor.execute(query)
                        except mysql.connector.Error as e:
                            time.sleep(5)
                            self.reconnect()
                            self.cursor.execute(query)
                        row_exists = self.cursor.fetchone() is not None

                        if row_exists == None or row_exists == False:
                            query = f'INSERT INTO {self.table_name} (name, phone, location, profile) VALUES ("{name.strip()}", "{phone.strip()}", "{location.strip()}", "{profile.strip()}")'
                            self.cursor.execute(query)
                            self.cnx.commit()
                            print(f"Crawler Logs: Company: {name.strip()} scrapped successfully.")
                        else:
                            print('Exist row: company: '+name.strip()+' phone: '+phone.strip())
                            self.cursor.fetchall()

                    no = no + 1
                    
                except Exception as e:
                    print(f"Crawler Error: Something went wrong. Error: ", str(e))
                    self.reconnect()
                    # self.driver.quit()
                    # no = no + 1
                    pass
                # init_company = init_company + 1
        
        # self.save_csv_file(result, "Results")
        # self.start_company = 0
        return soup

    def google_map_inner_link_scrap(self, url):
        
        session = requests_html.HTMLSession()
        response = session.get(url)
        response.html.render()
        inner_link_html = BeautifulSoup(response.html.html, "lxml")
        name = inner_link_html.find('h1', 'DUwDvf lfPIob').text
        print(f"Crawler Logs: Company: {name} scrapped successfully.")

        location = inner_link_html.find(
            'div', 'Io6YTe fontBodyMedium kR99db').text
        location = self.extract_zip_code(location)
    
        try:
            phone = inner_link_html.find_all(
                'div', 'Io6YTe fontBodyMedium kR99db')[2].text
            
            check_percentage = self.calculate_percentage_of_numbers(phone)
            if check_percentage<20:
                phone = inner_link_html.find_all(
            'div', 'Io6YTe fontBodyMedium kR99db')[3].text
                
            check_percentage = self.calculate_percentage_of_numbers(phone)
            if check_percentage<20:
                phone = inner_link_html.find_all(
            'div', 'Io6YTe fontBodyMedium kR99db')[4].text
                
            check_percentage = self.calculate_percentage_of_numbers(phone)
            if check_percentage<20:
                phone = "Not Available"
                
        except:
            phone = "Not Available"

        session.close()
        return name,phone,location

    def wait_for_element_location_to_be_stable(self, element):
        initial_location = element.location
        previous_location = initial_location
        start_time = time.time()
        while time.time() - start_time < 1:
            current_location = element.location
            if current_location != previous_location:
                previous_location = current_location
                start_time = time.time()
            time.sleep(1)

    def calculate_percentage_of_numbers(self,phone):
        total_characters = len(phone)
        digit_count = sum(1 for char in phone if char.isdigit())
        percentage = (digit_count / total_characters) * 100
        return percentage

    def extract_zip_code(self,address):
        zip_code_pattern = r'\b\d{5}(?:-\d{4})?\b'
        matches = re.findall(zip_code_pattern, address)
        if matches:
            try:
                zip_code = matches[0]
                return zip_code
            except:
                return address
        else:
            return address

# try:
crawler = GoogleMaps()
time.sleep(5)
crawler.google_map_crawler()
# except:
#     print("Crawler Error: Somwthing went wrong")