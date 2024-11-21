"""
    Module
"""
import argparse
import maya
import calendar
import time
import logging
import urllib
import pandas as pd
import io
import json
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from http.cookiejar import CookieJar
from enum import StrEnum
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementNotVisibleException, ElementNotSelectableException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import (
    WebDriverWait, Select
)
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.remote.webelement import (
    WebElement
)

class GiovanniPlotTypes(StrEnum):
    """
        <select id="serviceSelect" name="plotTypeList" class="pickerContent" style="display: none;">
        <option value="TmAvMp" data-class="" title="Interactive map of average over time at each grid cell">Time Averaged Map</option>
        <option value="QuCl" data-class="" title="Recurring daily, monthly, or seasonal averages over a range of years (one map per year)">Map, Recurring Averages</option>
        <option value="TmAvOvMp" data-class="" title="Interactive Overlay map of average over time at each grid cell">Time Averaged Overlay Map</option>
        <option value="AcMp" data-class="" title="Accumulation of measurement over time at each grid point">Map, Accumulated</option>
        <option value="MpAn" data-class="" title="Map animated along the chosen timeline for each grid cell">Animation</option>
        <option value="DiTmAvMp" data-class="" title="Difference of two time averaged variable maps">Map, Difference of Time Averaged</option></optgroup>
        
        <option value="CoMp" data-class="" title="Simple linear regression of 2 variables at each grid cell">Map, Correlation</option>
        <option value="ArAvSc" data-class="" title="Scatter plot comparing area averaged time series for two variables">Scatter, Area Averaged (Static)</option>
        <option value="IaSc" data-class="" title="Interactive Scatter">Scatter (Interactive)</option>
        <option value="StSc" data-class="" title="Static Scatter">Scatter (Static)</option>
        <option value="TmAvSc" data-class="" title="Time-averaged, interactive X-Y plot of 2 variables">Scatter, Time-Averaged (Interactive)</option>
        
        <option value="DiArAvTs" data-class="" title="Time series of area averages of differences between two variables at each spatial grid point">Time Series, Area-Averaged Differences</option>
        <option value="ArAvTs" data-class="" title="Time series of area-averaged values">Time Series, Area-Averaged</option>
        <option value="HvLt" data-class="" title="Longitude-averaged Hovmoller, plotted over the selected time and latitude ranges">Hovmoller, Longitude-Averaged</option>
        <option value="HvLn" data-class="" title="Latitude-averaged Hovmoller, plotted over the selected time and longitude ranges">Hovmoller, Latitude-Averaged</option>
        <option value="InTs" data-class="" title="Time series of recurring daily, monthly, or seasonal averages over a range of years">Time Series, Recurring Averages</option>
        
        <option value="HiGm" data-class="" title="Distribution of values over time and space">Histogram</option>
        <option value="ZnMn" data-class="" title="Zonal mean plot, averaged values are plotted over latitude zones">Zonal Mean</option>
        
        <option value="CrLt" data-class="" title="Cross Section, Latitude-Pressure">Cross Section, Latitude-Pressure</option>
        <option value="CrLn" data-class="" title="Cross Section, Longitude-Pressure">Cross Section, Longitude-Pressure</option>
        <option value="CrTm" data-class="" title="Cross Section, Time-Pressure">Cross Section, Time-Pressure</option>
        <option value="VtPf" data-class="" title="Vertical Profile">Vertical Profile</option>
    """
    TmAvMp = "Time Averaged Map"
    QuCl = "Map, Recurring Averages"
    TmAvOvMp = "Time Averaged Overlay Map"
    AcMp = "Map, Accumulated"
    MpAn = "Animation"
    DiTmAvMp = "Map, Difference of Time Averaged"
    CoMp = "Map, Correlation"
    ArAvSc = "Scatter, Area Averaged (Static)"
    IaSc = "Scatter (Interactive)"
    StSc = "Scatter (Static)"
    TmAvSc = "Scatter, Time-Averaged (Interactive)"
    DiArAvTs = "Time Series, Area-Averaged Differences"
    ArAvTs = "Time Series, Area-Averaged"
    HvLt = "Hovmoller, Longitude-Averaged"
    HvLn = "Hovmoller, Latitude-Averaged"
    InTs = "Time Series, Recurring Averages"
    HiGm = "Histogram"
    ZnMn = "Zonal Mean"
    CrLt = "Cross Section, Latitude-Pressure"
    CrLn = "Cross Section, Longitude-Pressure"
    CrTm = "Cross Section, Time-Pressure"
    VtPf = "Vertical Profile"


class GiovanniShapeGroups(StrEnum):
    """
        From shape selector
    """
    USSTD_COUNTRIES = "Countries and Areas"
    WWF_LAKES = "Lakes and Reservoirs"
    GESDISC_LAND = "Land Only file"
    GESDISC_SEA = "Sea Only file"
    TIGERLINE_USSTATES = "US States"
    FAO_WATERS = "Watersheds"
    ESRI_REGIONS = "World_Regions"

    @staticmethod
    def from_shape_string(value:str):
        if value.startswith("Countries and Areas"):
            return GiovanniShapeGroups.USSTD_COUNTRIES
        elif value.startswith("Lakes and Reservoirs"):
            return GiovanniShapeGroups.WWF_LAKES
        elif value.startswith("Land Only file"):
            return GiovanniShapeGroups.GESDISC_LAND
        elif value.startswith("Sea Only file"):
            return GiovanniShapeGroups.GESDISC_SEA
        elif value.startswith("US States"):
            return GiovanniShapeGroups.TIGERLINE_USSTATES
        elif value.startswith("Watersheds"):
            return GiovanniShapeGroups.FAO_WATERS
        elif value.startswith("World_Regions"):
            return GiovanniShapeGroups.ESRI_REGIONS
        return None



class Giovanni:
    """
        Giovanni Object
        driver = webdriver.Chrome()
        driver.get("https://giovanni.gsfc.nasa.gov/giovanni")

        print(driver.title)


        # Find login button and click it
        elems = driver.find_elements(
            by=By.XPATH, value="//*[contains(text(), 'Login')]")

        print(len(elems))

        for elem in elems:
            print(elem.id, elem.text)
            print(elem.get_attribute("href"))

        #storing current window handle
        main_page = driver.current_window_handle

        if (len(elems)>0):
            elem = elems[0]
            elem.click()

        # Fill username
        elems2 = driver.find_elements(
            by=By.XPATH, value="//*[contains(text(), 'Username')]/following::input"
        )
        print(len(elems2))

        for elem in elems2:
            print(elem.id)
        if (len(elems2)>0):
            elem = elems2[0]
            elem.send_keys(myusername)

        # Fill password
        elems2 = driver.find_elements(
            by=By.XPATH, value="//*[contains(text(), 'Password')]/following::input"
        )
        print(len(elems2))

        for elem in elems2:
            print(elem.id)
        if (len(elems2)>0):
            elem = elems2[0]
            elem.send_keys(mypass)
        
        driver.quit()
    """
    def __init__(self, user_name=None, password=None) -> None:
        self.giovanni_root = "https://giovanni.gsfc.nasa.gov/giovanni"
        self.user_name = user_name
        self.password = password
        self.driver = None
    
    def __enter__(self):
        self._init_driver()
        self.login_status = self.login(
            user_name=self.user_name,
            password=self.password)
        return self

    def _init_driver(self) -> None:
        self.driver = webdriver.Chrome() #.Firefox()
        self.driver.get(self.giovanni_root)
        # implicit waiting - switch to explicit time wait?
        #self.driver.implicitly_wait(5)

    def _find_button_by_label(self, label) -> (WebElement | None):
        label_pattern = f"//button[contains(text(), '{label}')]"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        if (len(elems)>0):
            elem = elems[0]
            return elem
        return None        

    def _print_input_field(self, input_type:str=None) -> None:
        label_pattern = "//input"
        if input_type:
            label_pattern = f"//input[@type='{input_type}']"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        for elem in elems:
            print(elem.get_attribute("outerHTML"))


    def _find_input_field_after_label(self,label) -> (WebElement | None):
        label_pattern = f"//label[contains(text(), '{label}')]/following-sibling::input"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        if (len(elems)>0):
            elem = elems[0]
            return elem
        return None

    def _find_input_field_before_label(self,label) -> (WebElement | None):
        label_pattern = f"//label[contains(text(), '{label}')]/preceding-sibling::input"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        if len(elems)>0:
            elem = elems[0]
            return elem
        return None

    def _find_input_field_by_atribute(self,attribute, att_value) -> (WebElement | None):
        label_pattern = f"//input[@{attribute}='{att_value}']"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        if len(elems)>0:
            elem = elems[0]
            return elem
        return None

    def _find_element_by_atribute(self,tag:str, attribute:str, att_value:str) -> (WebElement | None):
        label_pattern = f"//{tag}[@{attribute}='{att_value}']"
        elems = self.driver.find_elements(
            by=By.XPATH, value=label_pattern)
        if len(elems)>0:
            elem = elems[0]
            return elem
        return None


    def _handle_alert(self, alert_message:str=None)->None:
        try:
            WebDriverWait(driver=self.driver, timeout=5).until(EC.alert_is_present(), alert_message)
            alert = self.driver.switch_to.alert
            print(alert.text)
            alert.accept()
        except TimeoutException as ee:
            logging.error("Handling alert '"+alert_message+"':"+repr(ee))
    
    def _detect_and_handle_alert(
            self, alert_text:str=None)->tuple:
        """
            Parameters:
                alert_text(str): Expected partial text in alert message.

            Returns: (status, alert_text)
                    status(bool): True - Got expected alert or no alert (alert_tex will be None). False - Got unexpected alert.
                    alert_text(str): Returned alert text.
        """
        try:
            WebDriverWait(driver=self.driver, timeout=5).until(EC.alert_is_present())
            alert = self.driver.switch_to.alert
            alert_msg = alert.text
            alert.accept()
            if not alert_text:
                return (False, alert_msg)
            if alert_text in alert_msg:
                return (True, alert_msg)
            return (False, alert_msg)
        except TimeoutException as ee:
            #logging.error("Handling alert '"+alert_text+"':"+repr(ee))
            return (True, None)

    def _find_element_by_id(self, value=""):
        try:
            #status = EC.presence_of_element_located((By.ID, value))
            #if status:
            #    return True
            #else:
            #    return False
            elem = self.driver.find_element(
                by=By.ID, value=value
            )
            return elem
        except ValueError as ee:
            logging.error("Finding element with ID='"+value+"':"+repr(ee))
            return None
        except NoSuchElementException as ee:
            logging.error("Finding element with ID='"+value+"':"+repr(ee))
            return None

    def download_from_earthdata(
            self,url_str:str, username:str, password:str):
        # Create a password manager to deal with the 401 reponse that is returned from
        # Earthdata Login

        password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, "https://urs.earthdata.nasa.gov", username, password)


        # Create a cookie jar for storing cookies. This is used to store and return
        # the session cookie given to use by the data server (otherwise it will just
        # keep sending us back to Earthdata Login to authenticate).  Ideally, we
        # should use a file based cookie jar to preserve cookies between runs. This
        # will make it much more efficient.

        cookie_jar = CookieJar()


        # Install all the handlers.

        opener = urllib.request.build_opener(
            urllib.request.HTTPBasicAuthHandler(password_manager),
            #urllib.request.HTTPHandler(debuglevel=1),    # Uncomment these two lines to see
            #urllib.request.HTTPSHandler(debuglevel=1),   # details of the requests/responses
            urllib.request.HTTPCookieProcessor(cookie_jar))
        urllib.request.install_opener(opener)


        # Create and submit the request. There are a wide range of exceptions that
        # can be thrown here, including HTTPError and URLError. These should be
        # caught and handled.

        request = urllib.request.Request(url_str)
        response = urllib.request.urlopen(request)


        # Print out the result (not a good idea with binary data!)

        body = response.read()
        return body

    def logout(self) -> bool:
        elem = self.driver.find_element(
            by=By.ID, value="logoutLink"
        )
        if not elem:
            return False
        
        elem.click()
        conditions = [
            EC.presence_of_element_located((By.ID, "username")),
            EC.presence_of_element_located((By.ID, "password"))
        ]
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.all_of(*conditions))
        elem1=self._find_element_by_atribute(
            tag="a", attribute="href",
            att_value='https://giovanni.gsfc.nasa.gov/giovanni/')
        if not elem1:
            return False
        elem1.click()
        return True

    def login(self, user_name, password) -> bool:
        """
            Log in Eaethdata
        """
        if self._find_element_by_id(value="logoutLink"):
            self.logout()

        if not user_name:
            return False
        if not password:
            return False

        if not self.driver:
            return False

        #visibility check of overlappying progressbar
        elem = self._find_element_by_id("progressModal")
        if elem:
            WebDriverWait(driver=self.driver, timeout=30).until(
                EC.invisibility_of_element((By.ID, 'progressModal')))

        #print(self.driver.page_source)
        # wait for 'loginButton' or 'loginLink'
        WebDriverWait(driver=self.driver, timeout=30).until(
            EC.element_to_be_clickable((By.ID, 'loginButton')))
        # Find login button and click it
        elem = self._find_element_by_id(value="loginButton")
        #._find_button_by_label("Login")
        if not elem:
            return False
        elem.click()

        conditions = [
            EC.presence_of_element_located((By.ID, "username")),
            EC.presence_of_element_located((By.ID, "password")),
            EC.presence_of_element_located((By.ID,"stay_in"))
        ]
        WebDriverWait(driver=self.driver, timeout=30).until(
            EC.all_of(*conditions))
        #Fill username
        #elem = self._find_input_field_after_label("Username")
        elem = self._find_element_by_id(value="username")
        if not elem:
            return False
        elem.send_keys(user_name)

        #Fill password
        #elem = self._find_input_field_after_label("Password")
        elem = self._find_element_by_id("password")
        if not elem:
            return False
        elem.send_keys(password)

        # Uncheck 
        #elem = self._find_input_field_before_label("Stay signed in")
        elem = self._find_element_by_id(value="stay_in")
        if (elem and elem.is_selected()):
            elem.click()
        
        # print out all button WebElement
        # self._print_input_by_type("button")

        # LOG IN
        elem = self._find_input_field_by_atribute("name", "commit")
        if (elem and elem.is_displayed()):
            elem.click()
        else:
            logging.error("'LOG IN' element may not be visible. ")
            return False
        WebDriverWait(driver=self.driver, timeout=50).until(
            EC.invisibility_of_element((By.ID, 'progressModal')))
        #time.sleep(3)
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.visibility_of_element_located((By.ID, 'loginButton')))

        #self._print_input_field()
        # LOG IN
        elem = self._find_element_by_id("loginButton")
        #._find_button_by_label("Login")
        if not elem:
            logging.error("No 'loginButton' found")
            return False
        elem.click()

        self._handle_alert("giovanni.gsfc.nasa.gov says")
 
        return True

    def select_plot_type(self, 
                    plot_type:GiovanniPlotTypes=GiovanniPlotTypes.ArAvTs)->bool:
        WebDriverWait(driver=self.driver,
                      timeout=20).until(
                          EC.invisibility_of_element((By.ID, "progressModal"))
                      )
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.element_to_be_clickable((By.ID, 'serviceSelect-button')))
        elem = self.driver.find_element(
            by=By.ID,
            value="serviceSelect-button"
        )
        if not elem:
            return False
        elem.click()
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.ID, plot_type.name)))
        elem = self.driver.find_element(
            By.ID,
            value=plot_type.name
        )
        if not elem:
            return False
        elem.click()
        return True

    def select_plot_start_date(
            self,
            start_date:maya.MayaDT)->bool:
        #main_window = self.driver.current_window_handle
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#startDateCalendarLink > .fa')))
        #'startDateCalendarLink')))
        elem = self.driver.find_element(
            By.CSS_SELECTOR,
            value="#startDateCalendarLink > .fa"
        )
        if not elem:
            return False
        time.sleep(5)
        #elem.click()
        ActionChains(self.driver).move_to_element(
            elem
        ).click().perform()

        #elems = self.driver.find_elements(
        #    By.XPATH,
        #    value='//select[starts-with(@id, "yearCalendar")]/../../../../../..'
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))

        # set year
        WebDriverWait(driver=self.driver,
                      timeout=30).until(
                          EC.presence_of_element_located((By.ID, "startDateCalendar_t"))
                      )
        elem0 = self.driver.find_element(
            by=By.ID,
            value="startDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.XPATH,
            value=".//select[starts-with(@id, 'yearCalendar')]"
        )
        #WebDriverWait(driver=self.driver, timeout=30).until(
        #    EC.presence_of_element_located((By.ID, 'yearCalendar_5')))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value="yearCalendar_5"
        #)
        #if not elem:
        #    return False
        sel_startyear = Select(elem)
        year_str = str(start_date.year)
        sel_startyear.select_by_visible_text(year_str)

        # set month
        #WebDriverWait(driver=self.driver, timeout=20).until(
        #    EC.presence_of_element_located((By.ID, 'monthCalendar_6')))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value="monthCalendar_6"
        #)
        #if not elem:
        #    return False
        #elem.click()
        WebDriverWait(driver=self.driver,
                      timeout=30).until(
                          EC.presence_of_element_located((By.ID, "startDateCalendar_t"))
                      )
        elem0 = self.driver.find_element(
            by=By.ID,
            value="startDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.XPATH,
            value=".//select[starts-with(@id, 'monthCalendar')]"
        )

        #elem = self.driver.find_element(
        #    By.XPATH,
        #    value="//option[@value='Aug']"
        #)
        #elem.click()
        sel_startmonth = Select(elem)
        month_str = calendar.month_abbr[start_date.month]
        sel_startmonth.select_by_value(month_str)

        # day
        # wait condition - dall_cell_str should occcur whhich may not be the one retrieved
        day_cell_str = 'startDateCalendar_t_cell'+str(start_date.day)
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.ID, day_cell_str)))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value=day_cell_str
        #)
        #if not elem:
        #    return False
        #elem.click()
        elem0 = self.driver.find_element(
            by=By.ID,
            value="startDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.LINK_TEXT,
            value=str(start_date.day)
        )
        #print(elem.get_attribute("outerHTML"))
        ActionChains(self.driver).move_to_element(elem).click().perform()

        return True

    def select_plot_end_date(
            self,
            end_date:maya.MayaDT)->bool:
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#endDateCalendarLink > .fa')))
        #'startDateCalendarLink')))
        elem = self.driver.find_element(
            By.CSS_SELECTOR,
            value="#endDateCalendarLink > .fa"
        )
        if not elem:
            return False
        #time.sleep(1)
        #elem.click()
        ActionChains(self.driver).move_to_element(
            elem
        ).click().perform()


        #WebDriverWait(driver=self.driver, timeout=20).until(
        #    EC.visibility_of_element_located((By.ID, 'endDateCalendarLink')))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value="endDateCalendarLink"
        #)
        #if not elem:
        #    return False
        #elem.click()

        #print(self.driver.page_source)

        #elems = self.driver.find_elements(
        #    By.XPATH,
        #    value='//select[starts-with(@id, "yearCalendar")]/../../../../../..'
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))
        # set year
        WebDriverWait(driver=self.driver,
                      timeout=30).until(
                          EC.presence_of_element_located((By.ID, "endDateCalendar_t"))
                      )
        elem0 = self.driver.find_element(
            by=By.ID,
            value="endDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.XPATH,
            value=".//select[starts-with(@id, 'yearCalendar')]"
        )
        #print(elem.get_attribute("innerHTML"))
        #WebDriverWait(driver=self.driver, timeout=20).until(
        #    EC.presence_of_element_located((By.XPATH, '//select[starts-with(@id, "yearCalendar_")]')))
        #elems = self.driver.find_elements(
        #    By.XPATH,
        #    value='//select[starts-with(@id, "yearCalendar_")]'
        #)
        #if not elems:
        #    return False
        #elem.click()
        #for elem in elems:
        #    print(elem.get_attribute("innherHTML"))


        #elem = self.driver.find_element(
        #    By.XPATH,
        #    value="//option[text()='2022']"
        #)
        #elem.click()
        #elem.location_once_scrolled_into_view
        #elem.click()
        sel_endyear = Select(elem)
        year_str = str(end_date.year)
        sel_endyear.select_by_visible_text(year_str)

        #print(self.driver.page_source)
        WebDriverWait(driver=self.driver,
                      timeout=30).until(
                          EC.presence_of_element_located((By.ID, "endDateCalendar_t"))
                      )
        elem0 = self.driver.find_element(
            by=By.ID,
            value="endDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.XPATH,
            value=".//select[starts-with(@id, 'monthCalendar')]"
        )
        #print(elem.get_attribute("outerHTML"))
        # set month
        #WebDriverWait(driver=self.driver, timeout=20).until(
        #    EC.visibility_of_element_located((By.ID, 'monthCalendar_14')))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value="monthCalendar_14"
        #)
        #if not elem:
        #    return False
        #elem.click()

        sel_endmonth = Select(elem)
        month_str = calendar.month_abbr[end_date.month]
        sel_endmonth.select_by_value(month_str)

        #elem = self.driver.find_element(
        #    By.XPATH,
        #    value="//option[@value='Aug']"
        #)
        #elem.click()

        # day
        # wait condition - dall_cell_str should occcur whhich may not be the one retrieved
        day_cell_str = 'endDateCalendar_t_cell'+str(end_date.day)
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.ID, day_cell_str)))
        #elem = self.driver.find_element(
        #    By.ID,
        #    value=day_cell_str
        #)
        #if not elem:
        #    return False
        elem0 = self.driver.find_element(
            by=By.ID,
            value="endDateCalendar_t"
        )
        elem = elem0.find_element(
            by=By.LINK_TEXT,
            value=str(end_date.day)
        )
        #print(elem.get_attribute("outerHTML"))
        ActionChains(self.driver).move_to_element(elem).click().perform()

        return True

    def select_plot_area_by_bbox(
            self, bbox_str=None)->bool:
        if not bbox_str:
            return False
        
        #elems = self.driver.find_elements(
        #    by=By.XPATH,
        #    value="//*[contains(text(), 'Region')]/following-sibling::*"
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))
        if not bbox_str:
            return False
        elem = self._find_element_by_id(value="sessionDataSelBbPkbbox")
        if not elem:
            return False
        cur_val = elem.get_attribute("value")
        if not cur_val:
            elem.send_keys(bbox_str)
            return True
        # assume only bbox
        if not (";" in cur_val):
            elem.clear()
            elem.send_keys(bbox_str)
            return True
        cur_strs = cur_val.split(";")
        send_value = cur_strs[0]+";"+bbox_str
        elem.clear()
        elem.send_keys(send_value)
        return True

    def select_plot_area_by_shape(
            self, shape_str=None)->bool:
        """
            This is not working. Did not change actual value.
        """
        #elems = self.driver.find_elements(
        #    by=By.XPATH,
        #    value="//*[contains(text(), 'Region')]/following-sibling::*"
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))
        if not shape_str:
            return False
        elem = self._find_element_by_id(value="sessionDataSelBbPkbbox")
        if not elem:
            return False
        cur_val = elem.get_attribute("value")
        if not cur_val:
            elem.send_keys(shape_str+";")
            return True
        # assume only bbox
        if not (";" in cur_val):
            send_value = shape_str + ";" + cur_val
            elem.clear()
            elem.send_keys(send_value)
            return True
        cur_strs = cur_val.split(";")
        send_value = shape_str+";"+cur_strs[-1]
        elem.clear()
        elem.send_keys(send_value)
        return True

    def parse_shape_string(self, shape_str=None)->tuple:
        if not shape_str:
            return (None, None)
        group = GiovanniShapeGroups.from_shape_string(shape_str)
        if not group:
            return (None, shape_str)
        #print(group.value)
        group_len = len(group.value)+1
        shape_name = shape_str[group_len:]
        return group, shape_name

    def select_plot_area_by_shape_selector(
            self, shape_str=None)->bool:
        if not shape_str:
            return False
        shape_group, shape_name = self.parse_shape_string(shape_str=shape_str)
        #elems = self.driver.find_elements(
        #    by=By.XPATH,
        #    value="//*[contains(text(), 'Region')]/following-sibling::*"
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))
        main_page = self.driver.current_window_handle
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.visibility_of_element_located((By.ID, 'sessionDataSelBbPkshapeLink')))
        elem = self._find_element_by_id(value="sessionDataSelBbPkshapeLink")
        if not elem:
            return False
        elem.click()
        popup_page = None
        for handle in self.driver.window_handles:
            if handle != main_page:
                popup_page = handle
        
        if popup_page:
            self.driver.switch_to.window(popup_page)

        #time.sleep(5)
        #WebDriverWait(driver=self.driver, timeout=20).until(
        #    EC.visibility_of_element_located((By.XPATH, '*[starts-with(text(),"US States")]')))
        if not shape_group:
            logging.error("To-Be-Programmed for searching by shape name")
            return False
        shape_group_xpath = "//span[starts-with(text(), '"+shape_group.value+"')]/preceding-sibling::span"
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.visibility_of_element_located((By.XPATH, shape_group_xpath)))
        #print(self.driver.page_source)
        elem = self.driver.find_element(
            by=By.XPATH,
            value=shape_group_xpath
        )
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))
        #    if elem.is_displayed():
        #        elem.click()
        if not elem:
            return False
        #elem.click()
        ActionChains(self.driver).move_to_element(elem).click().perform()

        shape_name_xpath = "//ul[@style='display: block;']/li[@class='select2-results__option' and text()='"+shape_name+"']"
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.XPATH, shape_name_xpath)))
        elem = self.driver.find_element(
            by=By.XPATH,
            value=shape_name_xpath
        )
        if not elem:
            print("Error")
            return False

        #print(elem.get_attribute("outerHTML"))
        #parent_shape_name_xpath = "//ul[@style='display: block;']/li[@class='select2-results__option' and text()='"+"Michigan"+"']/.."
        #p_elem = self.driver.find_element(
        #    by=By.XPATH,
        #    value=parent_shape_name_xpath
        #)
        #print(p_elem.get_attribute("outerHTML"))
        
        # Scroll to the element using JavaScript
        #self.driver.execute_script("arguments[0].scrollTop += 100;", elem)
        #self.driver.execute_script("arguments[0].scrollIntoView();", elem)
        #self.driver.execute_script(
        #    'arguments[0].scrollIntoView({behavior: "instant", block: "start", inline: "start"});',
        #    elem)
        #The following solution not working - using Chrome for now.
        if 'firefox' in self.driver.capabilities['browserName']:
            self.firefox_scroll_shim(self.driver, elem)
        ActionChains(self.driver).move_to_element(elem).click().perform()
        #elem.click()

        #print(self.driver.page_source)
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.LINK_TEXT, "Close")))
        elem = self.driver.find_element(
            by=By.LINK_TEXT,
            value="Close"
        )
        if not elem:
            print("Error")
            return False
        #elem.location_once_scrolled_into_view
        elem.click()
        #self.driver.execute_script("arguments[0].click();", elem)
        return True

    def _scroll_down_until_elem_visible(
            self):
        SCROLL_PAUSE_TIME = 0.5

        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        

    def _print_long_element(
            self, elem:WebElement,
            max_len_per_print:int = 1000
    ):
        ss = elem.get_attribute("outerHTML")
        len_ss = len(ss)
        if len_ss < (max_len_per_print+100):
            print(ss)
        else:
            nloops = int(len_ss/max_len_per_print)
            sss_start = 0
            sss_end = 0
            for sss in range(nloops):
                sss_start = sss * max_len_per_print
                sss_end = (sss+1) * max_len_per_print
                print(ss[sss_start:sss_end])
                input("Press Enter for more......")
            if sss_end < len_ss:
                print(ss[sss_end:])

    def firefox_scroll_shim(
            self, passed_in_driver, object):
        x = object.location['x']
        y = object.location['y']
        scroll_by_coord = 'window.scrollTo(%s,%s);' % (
            x,
            y
        )
        scroll_nav_out_of_way = 'window.scrollBy(0, -120);'
        passed_in_driver.execute_script(scroll_by_coord)
        passed_in_driver.execute_script(scroll_nav_out_of_way)

    def select_plot_variable_by_keywords(
            self, var_str=None)->bool:
        if not var_str:
            return False
        # Fill the search with seartch_str
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.ID, "facetedSearchBarInput")))
        elem = self.driver.find_element(
            by=By.ID,
            value="facetedSearchBarInput"
        )
        if not elem:
            return False
        elem.send_keys(var_str)
        # click the search button
        WebDriverWait(driver=self.driver, timeout=20).until(
            EC.presence_of_element_located((By.ID, "facetedSearchButton")))
        elem = self.driver.find_element(
            by=By.ID,
            value="facetedSearchButton"
        )
        if not elem:
            return False
        elem.click()

        #print(elem.get_attribute("innerHTML"))

        #elems = self.driver.find_elements(
        #    by=By.XPATH,
        #    value="//label[text()='Variable']/../.."
        #)
        #for elem in elems:
        #    print(elem.get_attribute("innerHTML"))

        WebDriverWait(driver=self.driver, timeout=50).until(
            EC.presence_of_element_located((By.XPATH, "//a[@href='yui-dt0-href-varName']")))
        # click Variable column to sort it ascneding
        elem = self.driver.find_element(
            by=By.XPATH,
            value="//a[@href='yui-dt0-href-varName']"
        )
        #print(elem.get_attribute("title"))
        if (elem and elem.get_attribute("title")=="Click to sort ascending"):
            elem.click()
        #elem = self.driver.find_element(
        #    by=By.XPATH,
        #    value="//a[@href='yui-dt0-href-varName']"
        #)
        #print(elem.get_attribute("title"))
        #if (elem and elem.get_attribute("title")=="Click to sort ascending"):
        #    elem.click()
        #elem = self.driver.find_element(
        #    by=By.XPATH,
        #    value="//a[@href='yui-dt0-href-varName']"
        #)
        #print(elem.get_attribute("title"))
        #if (elem and elem.get_attribute("title")=="Click to sort ascending"):
        #    elem.click()

        #ActionChains(driver=self.driver).move_to_element(elem).click().perform()
        #elem.click()
        #print(self.driver.page_source)
        #self._print_input_field(input_type="checkbox")
        #print(elem.get_attribute("outerHTML"))
        #self._print_long_element(elem=elem)
        # get table
        # value="//input[@type='checkbox' and starts-with(@title, 'Select ')]/../../../../.."
        # get tbody
        # value="//input[@type='checkbox' and starts-with(@title, 'Select ')]/../../../.."
        # get tr
        # value="//input[@type='checkbox' and starts-with(@title, 'Select ')]/../../.."
        # get td
        # value="//input[@type='checkbox' and starts-with(@title, 'Select ')]/../.."
        time.sleep(1)
        elems = self.driver.find_elements(
            by=By.XPATH,
            value="//input[@type='checkbox' and starts-with(@title, 'Select ')]"
        )
        #print(len(elems))
        #for elem in elems:
        #    self._print_long_element(elem=elem)
        if (len(elems)>0):
            elem = elems[0]
            elem.click()
            return True
        return False

    def plot_data(self):
        WebDriverWait(driver=self.driver, timeout=50).until(
            EC.presence_of_element_located((By.ID, "sessionDataSelToolbarplotBTN-button")))
        elem = self.driver.find_element(
            by=By.ID,
            value="sessionDataSelToolbarplotBTN-button"
        )
        if not elem:
            return False
        #print(elem.get_attribute("outerHTML"))
        ActionChains(self.driver).move_to_element(elem).click().perform()
        #print(self.driver.page_source)
        alert_status, alert_text = self._detect_and_handle_alert()
        if not alert_status:
            print("Error: unexpected alert.")
            print(alert_text)
            return False
        return True

    def get_results_csv_url(self):
        """
            print("elem=", elem.get_attribute("outerHTML"))
            elem1 = self._find_element_by_id("progressBar")
            if elem1:
                print("bar=", elem1.get_attribute("outerHTML"))
            elem2 = self._find_element_by_id("progressModal")
            if elem2:
                print("elem2=", elem2.get_attribute("outerHTML"))
            elem1 = self._find_element_by_id("progressBar")
            if elem1:
                print("bar=", elem1.get_attribute("outerHTML"))

        elem = self._find_element_by_id("progressModal")
        if elem:
            print(elem.get_attribute("outerHTML"))
            WebDriverWait(driver=self.driver, timeout=30).until(
                EC.invisibility_of_element_located(locator=(By.ID, 'progressModal')))
            WebDriverWait(driver=self.driver, timeout=30).until(
                EC.invisibility_of_element_located(locator=(By.XPATH, "//div[@id='progressModal' and contains(@class, 'showProgress')]")))
        elem = self._find_element_by_id("progressBar")
        if elem:
            print(elem.get_attribute("outerHTML"))
            WebDriverWait(driver=self.driver, timeout=30).until(
                EC.invisibility_of_element_located(locator=(By.ID, 'progresssBar')))
            elems1 = self.driver.find_elements(
                by=By.XPATH,
                value="//*[contains(@id, 'progress')]")
            print("EMELESSSSSSS========")
            for elem1 in elems1:
                print("elem=", elem1.get_attribute("outerHTML"))
            print("ENDDDDDD========")
        if elem:
            print(elem.get_attribute("outerHTML"))
            WebDriverWait(driver=self.driver, timeout=30).until(
                EC.invisibility_of_element_located(locator=(By.ID, 'progresssSpinner')))


        """
        #visibility check of overlappying progressbar
        WebDriverWait(driver=self.driver,timeout=50).until(
            EC.invisibility_of_element((By.ID,"progressModal"))
        )
        WebDriverWait(driver=self.driver,timeout=300).until(
            EC.invisibility_of_element((By.ID,"progressBar"))
        )
        #WebDriverWait(driver=self.driver, timeout=30).until(
        #    EC.presence_of_element_located((By.ID, "sessionDataSelToolbarbackBTN-button")))
        #elem = self.driver.find_element(
        #    by=By.ID,
        #    value="sessionDataSelToolbarbackBTN-button"
        #)
        #if not elem:
        #    return None
        #elem.click()

        #Collapse History
        #WebDriverWait(driver=self.driver, timeout=30).until(
        #    EC.presence_of_element_located((By.ID, "sessionWorkspaceHistoryViewHistoryTreeTitle")))
        #elem = self.driver.find_element(
        #    by=By.ID,
        #    value="sessionWorkspaceHistoryViewHistoryTreeTitle"
        #)
        #ActionChains(self.driver).move_to_element(elem).click().perform()

        #print(elem.get_attribute("outerHTML"))


        #print(self.driver.page_source)
        #elems = self.driver.find_elements(
        #    by=By.XPATH,
        #    value="//*[contains(@class, 'hideProgress')]"
        #)
        #print("progress=", len(elems))
        #for elem in elems:
        #    print(elem.get_attribute("outerHTML"))

        #sessionWorkspaceExpand button
        # Expand all
        WebDriverWait(driver=self.driver, timeout=30).until(
            EC.presence_of_element_located((By.ID, "sessionWorkspaceExpand")))
        elem = self.driver.find_element(
            by=By.ID,
            value="sessionWorkspaceExpand"
        )
        ActionChains(self.driver).move_to_element(elem).click().perform()

        #print(elem.get_attribute("outerHTML"))
        #get download link
        WebDriverWait(driver=self.driver, timeout=50).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ygtvitem') and @id='ygtv1']/div[contains(@class,'ygtvchildren')]/div[contains(@class,'ygtvitem')]")))
        elems = self.driver.find_elements(
            by=By.XPATH,
            value="//div[contains(@class, 'ygtvitem') and @id='ygtv1']/div[contains(@class,'ygtvchildren')]/div[contains(@class,'ygtvitem')]"
        )
        print("number of elems=", len(elems))
        #for elem in elems:
        #    print(elem.get_attribute("outerHTML"))
        if not elems:
            return None
        elem = elems[0]
        
        if not elem:
            return None
        WebDriverWait(driver=elem, timeout=30).until(
            EC.visibility_of_element_located(locator=(By.XPATH, ".//*[text()='Downloads']"))
        )
        elems = elem.find_elements(by=By.XPATH, value=".//*[text()='Downloads']")
        if not elems:
            return None
        elem = elems[0]
        if not elem:
            return None
        ActionChains(self.driver).move_to_element(elem).click().perform()
        WebDriverWait(driver=self.driver, timeout=30).until(
            EC.visibility_of_element_located(locator=(By.LINK_TEXT, "CSV"))
        )
        elems = self.driver.find_elements(
            by=By.LINK_TEXT,
            value="CSV"
        )
        if not elems:
            return None
        elem = elems[0]
        if not elem:
            return None
        s_url = elem.get_attribute("href")
        elem = self.driver.find_element(
            by=By.XPATH,
            value="//div[contains(@class, 'ygtvitem') and @id='ygtv1']/div[contains(@class,'ygtvchildren')]/div[contains(@class,'ygtvitem')]"
        )
        if not elem:
            return None
        #print("--------")
        #print(elem.get_attribute("outerHTML"))
        elem1 = elem.find_element(
            by=By.XPATH,
            value=".//i[@title='Delete plot']"
        )
        if not elem1:
            return None
        ActionChains(self.driver).move_to_element(elem1).click().perform()
        self._handle_alert(alert_message="Are you sure you want to permanently delete this plot?")
        if not s_url:
            return None
        print("URL=", s_url)
        return s_url
    
    def _find_skip_index_by_keywords(
            self, csv_content_str:str,
            header_keyword:str=None):
        if not header_keyword:
            return -1
        sio = io.StringIO(csv_content_str)
        nline = -1
        while line := sio.readline():
            nline += 1
            if header_keyword in line:
                return nline
        return -1

    def save_to_csv_file(
            self, csv_content,
            csv_file:str,
            csv_skip_rows:int=-1,
            csv_skip_signature:str=None,
            csv_keep_metadata:bool=False,
            csv_sep:str=",",
            rename_column:str=None,
            rename_column_index:int=1,
            rename_column_old_name:str=None):
        csv_content_str = csv_content.decode('utf-8')
        skip_rows = -1
        if csv_skip_rows > 0:
            skip_rows = csv_skip_rows
        elif csv_skip_signature:
            skip_rows = self._find_skip_index_by_keywords(
                csv_content_str=csv_content_str,
                header_keyword=csv_skip_signature)
        if skip_rows>0:
            df = pd.read_csv(io.StringIO(csv_content_str),
                             skiprows=skip_rows,sep=csv_sep)
        else:
            df = pd.read_csv(io.StringIO(csv_content_str), sep=csv_sep)
        giovanni_util_metadata = list()
        if rename_column:
            if rename_column_old_name:
                df = df.rename(columns={rename_column_old_name:rename_column})
                _elem = dict()
                _elem['key'] = "gu_rename_old_col_name"
                _elem['value'] = rename_column_old_name
                giovanni_util_metadata.append(_elem)
                _elem = dict()
                _elem['key'] = "gu_rename_new_col_name"
                _elem['value'] = rename_column
                giovanni_util_metadata.append(_elem)
            else:
                old_col_name = df.columns[rename_column_index]
                df = df.rename(columns={old_col_name:rename_column})
                _elem = dict()
                _elem['key'] = "gu_rename_old_col_name"
                _elem['value'] = old_col_name
                giovanni_util_metadata.append(_elem)
                _elem = dict()
                _elem['key'] = "gu_rename_new_col_name"
                _elem['value'] = rename_column
                giovanni_util_metadata.append(_elem)
        print(df)
        df.to_csv(csv_file, index=False)
        if csv_keep_metadata:
            self._write_csv_metadata(
                csv_meta_file=csv_file+".metadata",
                csv_content_str=csv_content_str,
                skip_rows=skip_rows,
                csv_sep=csv_sep,
                gu_metadata=giovanni_util_metadata)

    def _write_csv_metadata(
            self,
            csv_meta_file:str,
            csv_content_str:str,
            skip_rows:int=-1,
            csv_sep:str=",",
            gu_metadata:list=[]):
        if not csv_content_str:
            return
        sio = io.StringIO(csv_content_str)
        with open(csv_meta_file, "w") as f:
            nline = -1
            while line := sio.readline():
                nline += 1
                if skip_rows < nline:
                    break
                f.write(line)
            for kv in gu_metadata:
                f.write(f"{kv['key']}{csv_sep}{kv['value']}\n")

    def save_to_parquet_file(
            self, csv_content,
            parquet_file:str,
            csv_skip_rows:int=-1,
            csv_skip_signature:str=None,
            csv_keep_metadata:bool=False,
            csv_sep:str=",",
            rename_column:str=None,
            rename_column_index:int=1,
            rename_column_old_name:str=None):
        csv_content_str = csv_content.decode('utf-8')
        skip_rows = -1
        if csv_skip_rows > 0:
            skip_rows = csv_skip_rows
        elif csv_skip_signature:
            skip_rows = self._find_skip_index_by_keywords(
                csv_content_str=csv_content_str,
                header_keyword=csv_skip_signature)
        if skip_rows>0:
            df = pd.read_csv(io.StringIO(csv_content_str),
                             skiprows=skip_rows,sep=csv_sep)
        else:
            df = pd.read_csv(io.StringIO(csv_content_str), sep=csv_sep)

        giovanni_util_metadata = dict()
        if rename_column:
            if rename_column_old_name:
                df = df.rename(columns={rename_column_old_name:rename_column})
                _rename = dict()
                _rename["old_col_name"] = rename_column_old_name
                _rename["new_col_name"] = rename_column
                giovanni_util_metadata["rename"] = _rename
            else:
                old_col_name = df.columns[rename_column_index]
                df = df.rename(columns={old_col_name:rename_column})
                _rename = dict()
                _rename["old_col_name"] = old_col_name
                _rename["new_col_name"] = rename_column
                giovanni_util_metadata["rename"] = _rename

        pt = pa.Table.from_pandas(df)

        if csv_keep_metadata:
            pt = self._append_parquet_metadata(
                parquet_table=pt,
                csv_content_str=csv_content_str,
                skip_rows=skip_rows,
                csv_sep=csv_sep,
                gu_metadata=giovanni_util_metadata)

        pq.write_table(table=pt,
                       where=parquet_file)

    def _append_parquet_metadata(
            self,
            parquet_table,
            csv_content_str:str,
            skip_rows:int=-1,
            csv_sep:str=",",
            gu_metadata:dict={}):
        if not csv_content_str:
            return parquet_table
        if skip_rows < 1:
            return parquet_table
        sio = io.StringIO(csv_content_str)
        soo = io.StringIO()
        nline = -1
        while line := sio.readline():
            nline += 1
            if skip_rows < nline:
                break
            soo.write(line+"\n")
        soo.seek(0)
        df = pd.read_csv(filepath_or_buffer=soo,
                         sep=csv_sep, header=None)
        add_metadata = {}
        new_metadata = {b'csv_metadata': b'{}'}
        num_columns = len(df.columns)
        if num_columns != 2:
            return parquet_table
        # Using iterrows()
        for index, row in df.iterrows():
            column_name = row[0]
            print("row0=",row[0], "row1=", row[1])
            if column_name.endswith(":"):
                ncolumn_name = column_name[:-1]
                column_value = row[1]
                add_metadata[ncolumn_name] = column_value
        json_str = json.dumps(add_metadata)
        bytes_obj = bytes(json_str, "utf-8")
        new_metadata[b'csv_metadata'] = bytes_obj
        if gu_metadata:
            json_str = json.dumps(gu_metadata)
            bytes_obj = bytes(json_str, "utf-8")
            new_metadata[b'gu_metadata'] = bytes_obj

        print("new_metadata=", new_metadata)
        merged_metadata = { **new_metadata, **(parquet_table.schema.metadata or {}) }
        print("merged_metadata=", merged_metadata)
        return parquet_table.replace_schema_metadata(merged_metadata)


    def __del__(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()
            self.driver = None

#--------main-----------
def get_args():
    parser = argparse.ArgumentParser(
        description="Giovanni Tool"
    )
    parser.add_argument("--plot-type",
                        dest="plot_type",
                        type=GiovanniPlotTypes,
                        choices=list(GiovanniPlotTypes),
                        metavar=[gpt.value for gpt in GiovanniPlotTypes])
    parser.add_argument("--plot-start-date",
                        dest="plot_start_date",
                        type=maya.parse)
    parser.add_argument("--plot-end-date",
                        dest="plot_end_date",
                        type=maya.parse)
    parser.add_argument("--earthdata-login-name",
                        dest="username",
                        type=str)
    parser.add_argument("--earthdata-login-pass",
                        dest="password",
                        type=str)
    parser.add_argument("--plot-area-bbox",
                        dest="plot_area_bbox",
                        type=str,
                        help="Region bounding box: West, South, East, North")
    parser.add_argument("--plot-area-shape",
                        dest="plot_area_shape",
                        type=str,
                        help="Shape string. It should start with the group name in exact spelling"
                        ", followed by shape name")
    parser.add_argument("--plot-variable",
                        dest="plot_variable",
                        type=str,
                        help="Variable keywords. It should uniquely identify one variable. "
                        "First match will be used.")
    parser.add_argument("--csv-separator",
                        dest="csv_separator",
                        type=str,
                        default=",",
                        help="Separator for CSV. Default = ',' ")
    parser.add_argument("--rename-column",
                        dest="rename_column",
                        type=str,
                        help="New name of the column at rename-column-index, which default to 1.")
    parser.add_argument("--rename-column-index",
                        dest="rename_column_index",
                        type=int,
                        default=1,
                        help="The index of the column to be edited - renamed.")
    parser.add_argument("--rename-column-old-name",
                        dest="rename_column_old_name",
                        type=str,
                        help="The name of the column to be edited - renamed. It take precedes rename-column-index.")
    parser.add_argument("--save-to-csv-file",
                        dest="save_to_csv_file",
                        type=str,
                        help="Save to a CSV file if given the full file path "
                        "to be written into.")
    parser.add_argument("--save-to-csv-file-metadata",
                        dest="save_to_csv_file_metadata",
                        action='store_true',
                        help="Save metadata to a metadata file of the CSV with extension .metadata.")
    parser.add_argument("--csv-skip-rows",
                        dest="csv_skip_rows",
                        type=int,
                        default=-1,
                        help="Number of rows to be skipped.")
    parser.add_argument("--csv-skip-signature",
                        dest="csv_skip_signature",
                        type=str,
                        help="Keyword or phrase contained in the start header line.")
    parser.add_argument("--save-to-parquet-file",
                        dest="save_to_parquet_file",
                        type=str,
                        help="Save to a Parquet file if given the full file path "
                        "to be written into.")
    args = parser.parse_args()
    return args

def giovanni_main()->bool:
    args = get_args()
    with Giovanni() as gv:
        #print("object inited")
        #print("current login status =", gv.login_status)

        gv.login_status = gv.login(user_name=args.username, password=args.password)
        #gv.login_status = gv.login(user_name=args.username, password=args.password)
        #print("after login status=", gv.login_status)
        # print(gv.driver.page_source)
        if not gv.select_plot_type(plot_type=args.plot_type):
            return False

        if not gv.select_plot_start_date(start_date=args.plot_start_date):
            return False
        
        if not gv.select_plot_end_date(end_date=args.plot_end_date):
            return False

        if args.plot_area_bbox:
            if not gv.select_plot_area_by_bbox(bbox_str=args.plot_area_bbox):
                return False
        if args.plot_area_shape:
            if not gv.select_plot_area_by_shape_selector(shape_str=args.plot_area_shape):
                return False
        if args.plot_variable:
            if not gv.select_plot_variable_by_keywords(var_str=args.plot_variable):
                return False
        if not gv.plot_data():
            return False
        csv_url = gv.get_results_csv_url()
        if not csv_url:
            return False
        csv_content = gv.download_from_earthdata(
            url_str=csv_url,
            username=args.username,
            password=args.password)
        #print(csv_content)
        if not csv_content:
            return False
        #save to CSV file
        if args.save_to_csv_file:
            gv.save_to_csv_file(
                csv_content=csv_content,                
                csv_file=args.save_to_csv_file,
                csv_keep_metadata=args.save_to_csv_file_metadata,
                csv_skip_rows=args.csv_skip_rows,
                csv_skip_signature=args.csv_skip_signature,
                csv_sep=args.csv_separator,
                rename_column_old_name=args.rename_column_old_name,
                rename_column=args.rename_column,
                rename_column_index=args.rename_column_index)
        if args.save_to_parquet_file:
            gv.save_to_parquet_file(
                csv_content=csv_content,                
                parquet_file=args.save_to_parquet_file,
                csv_keep_metadata=args.save_to_csv_file_metadata,
                csv_skip_rows=args.csv_skip_rows,
                csv_skip_signature=args.csv_skip_signature,
                csv_sep=args.csv_separator,
                rename_column_old_name=args.rename_column_old_name,
                rename_column=args.rename_column,
                rename_column_index=args.rename_column_index)
        """
        if args.plot_area_shape:
            if not gv.select_plot_area_by_shape(shape_str=args.plot_area_shape):
                return False

        print(gv.driver.page_source)
        print(args.plot_start_date)
        print(args.plot_end_date)

        #Test destroy
        #gv.__del__()
        gv = None
        """
    print("Done")
    return True
if __name__ == '__main__':
    if not giovanni_main():
        print("Failed.")
    else:
        print("Success!")