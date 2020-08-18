from pyetltools.core.connector import Connector
import time

class RemedyConnector(Connector):
    def __init__(self, key, url):
        super().__init__(key)
        self.url = url

    def remove_prefix(self, s, prefix):
        return s[len(prefix):] if s.startswith(prefix) else s

    def get_text_for_label(self, driver, label_text, accept_empty=True):
        i=5
        found=False
        res=None
        while i>0:
            try:
                print("Looking for label "+label_text)
                e=driver.find_element_by_xpath(f"//label[contains(text(),'{label_text}')]")
                print("Element found")
                text_id=self.remove_prefix(e.get_attribute("for"),"x-")
                print("Looking for element: "+text_id)
                t=driver.find_element_by_id(text_id)
                print("Element found")
                res=None
                try:
                    res=t.get_attribute('value')
                except Exception as e2:
                    print(e2)
                    print("Cannot get value from element.Trying text.")
                    res=t.text
                print("Value (or text):"+ res if res else "(None)")
                if (res and res!="") or accept_empty:
                    i=0
                    found=True
                else:
                    print("Exception - element empty")
                    time.sleep(3)
            except Exception as e:
                print(e)
                print("Exception")
                time.sleep(3)

            i = i - 1
        if not found:
            raise Exception("Element not found or text empty")
        return res

    def get_remedy_ticket_info(self, remedy_ticket_no, headless=True):
        driver=None
        try:
            from selenium import webdriver
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.chrome.options import Options

            options = Options()
            options.headless = headless

            driver = webdriver.Chrome(chrome_options=options)
            ticket_url=self.url.format(cr=remedy_ticket_no )
            print("Opening: "+ticket_url)
            driver.get(ticket_url)
            time.sleep(3)
            try:
                window_before = driver.window_handles[0]
                obj = driver.switch_to.alert
                obj.accept()
                driver.switch_to.window(window_before)
                print("Alert accepted")
            except Exception as x:
                print("OK: no alert to switch to."+str(x))
            time.sleep(3)
            ret=dict(change_id= self.get_text_for_label(driver, "Change ID" , False),
                     coordinator_group=self.get_text_for_label(driver, "Coordinator Group"),
                     change_coordinator=self.get_text_for_label(driver, "Change Coordinator"),
                     change_location=self.get_text_for_label(driver, "Change Location"),
                     service=self.get_text_for_label(driver, "Service"),
                     template=self.get_text_for_label(driver, "Template"),
                     summary=self.get_text_for_label(driver, "Summary"),
                     notes=self.get_text_for_label(driver, "Notes"),
                     change_class=self.get_text_for_label(driver, "Class"),
                     change_reason=self.get_text_for_label(driver, "Change Reason"),
                     target_date=self.get_text_for_label(driver, "Target Date"),
                     impact=self.get_text_for_label(driver, "Impact"),
                     urgency=self.get_text_for_label(driver, "Urgency"),
                     priority=self.get_text_for_label(driver, "Priority"),
                     risk_level=self.get_text_for_label(driver, "Risk Level"),
                     status=self.get_text_for_label(driver, "Status"),
                     status_reason=self.get_text_for_label(driver, "Status Reason"),
                     manager_group=self.get_text_for_label(driver, "Manager Group"),
                     change_group=self.get_text_for_label(driver, "Change Manager"),
                     vendor_group=self.get_text_for_label(driver, "Vendor Group"),
                     vendor_ticket_number = self.get_text_for_label(driver, "Vendor Ticket Number")
                     )

            jn_info = driver.find_element_by_link_text("JN info")
            jn_info.click()
            time.sleep(3)
            jn_info=dict()

            text_area = driver.find_element_by_id("arid_WIN_1_536870950")
            time.sleep(3)


            row_i=0
            column_names=None
            while True:
                table = driver.find_element_by_id("T536870947")
                rows=table.find_elements_by_css_selector("table > tbody > tr")
                if row_i==len(rows):
                    break
                row=rows[row_i]

                if row_i==0:
                    v = [td.get_attribute('textContent') for td in row.find_elements_by_xpath(".//th")]
                    if len(v)==3:
                        column_names=v
                    row_i += 1
                    continue
                else:
                    v = [td.text for td in row.find_elements_by_xpath(".//td")]
                row_i += 1
                name, text, mandatory=v
                row.click()
                text_area_txt=text_area.get_attribute("value")
                i=5
                import re
                pattern = re.compile(r'\s+')
                text_area_txt_s = re.sub(pattern, '', text_area_txt)
                text_s = re.sub(pattern, '', text)
                while not text_area_txt_s.startswith(text_s):
                    print(text)
                    print("text_area"+text_area_txt)
                    text_area_txt = text_area.get_attribute("value")
                    text_area_txt_s = re.sub(pattern, '', text_area_txt)
                    text_s = re.sub(pattern, '', text)
                    time.sleep(1)
                    i-=1
                    if i==0:
                        raise Exception("Max retries reached.")
                if column_names:
                    jn_info[name]=dict(zip(column_names, (text, mandatory, text_area_txt)))
                else:
                    jn_info[name] =  (text, mandatory, text_area_txt)
            ret["JN_info"]=jn_info

            work_detail = driver.find_element_by_link_text("Work Detail")
            work_detail.click()
            time.sleep(3)
            work_detail=[]
            table = driver.find_element_by_id("T301389923")
            for row in table.find_elements_by_css_selector("table > tbody > tr")[1:]:
                type, notes, files, submit_date, submitter= [td.text for td in row.find_elements_by_xpath(".//td")]
                work_detail.append(dict(type=type, notes=notes, files=files, submit_date=submit_date, submitter=submitter))
            ret["work_detail"]=work_detail

            jn_info = driver.find_element_by_link_text("Date/System")
            jn_info.click()
            time.sleep(3)
            #date_system=dict()
            #table = driver.find_element_by_id("T536870947")
            #for row in table.find_elements_by_css_selector("table > tbody > tr")[1:]:
            #    name, text, mandatory = [td.text for td in row.find_elements_by_xpath(".//td")]
            #    jn_info[name]=(text, mandatory)
            #ret["JN_info"]=jn_info
            labels=dict()
            form_date_system = driver.find_element_by_id("WIN_1_302058000")
            for label in form_date_system.find_elements_by_css_selector("label"):
                print(label.text)
                id=self.remove_prefix(label.get_attribute("for"),"x-")
                print("Looking for element: "+id)
                try:
                    t=driver.find_element_by_id(id)
                    print(label.text + " " + t.get_attribute("value"))
                    labels[label.text] = t.get_attribute("value")
                except:
                    print("Cannot find element with id:"+id)

            ret["labels"]=labels
            return ret
        except Exception as e:
            raise e
        finally:
            if driver:
                pass
                logout_btn=driver.find_element_by_id("WIN_0_300000044")
                logout_btn.click()
                print("Logout clicked.")
                time.sleep(5)
                driver.quit()


    def validate_config(self):
        super().validate_config()