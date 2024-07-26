# This is a general scraper script I developed to pull table definitions
# from servicetitan for a semantic layer. There was no API and Servicetitan
# unfortunately uses primarily dynamic generation so I had to use Selenium.

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, NavigableString

import pandas as pd
import re

def contains_ui_label(list_of_strings):
    """Checks if any string in the list contains "ui label".

    **generated using Gemini 1.5

    Args:
    list_of_strings: A list of strings.

    Returns:
    True if any string contains "ui label", False otherwise.
    """

    for string in list_of_strings:
        if "ui label" in string:
            return True
    return False

def login(username, password, link):
    driver = webdriver.Chrome()
    driver.get(link)

    # Find login elements (adjust locators as needed)
    wait = WebDriverWait(driver, 20)  # Adjust the timeout as needed
    username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
    password_field = driver.find_element(By.NAME, "password")
    login_button = driver.find_element(By.XPATH, "//button[text()='Sign In']")

    # Enter credentials
    username_field.send_keys(username)
    password_field.send_keys(password)

    # Submit the form
    login_button.click()

    # Handle potential delays or redirects
    wait.until(EC.presence_of_element_located((By.ID, "kpi-matrix-grid")))  # Adjust wait time as necessary

    # Perform actions after login
    # ...
    html_content = driver.page_source
    driver.quit()

    return html_content

def scrape_kpi_matrix_page(username,password,url=None):

    # default test url
    if not url:
       url = "https://go.servicetitan.com/#/KpiMatrix/Jobs"
    soup = BeautifulSoup(login(username,password,url), "html.parser")
    results = soup.find(id="kpi-matrix-grid")
    job_elements = results.find_all('td', role='gridcell')


    mdef = {}

    # Loop through page elements and clean data into the mdef (metric definition) dictionary
    # (Previously I used a next neighbors method but because of <b> tags, it became difficult to parse.)
    for job_element in job_elements:
        if not (job_element and isinstance(job_element,NavigableString)):
            ew = '<td class="" role="gridcell">'
            ew2 = '<br/>'
            job_element =  str(job_element).replace(ew,'')
            pattern = r">(.+?)<"
            matchs = re.finditer(pattern, job_element)
            elements_text = []
            for match in matchs:
                extracted_text = match.group(1)
                extracted_text = extracted_text.replace(ew2,'')
                elements_text.append(extracted_text)
            elements_text = list(filter(lambda x: x.strip(), elements_text))

            if not contains_ui_label(elements_text):
                metric = elements_text[0]
                definition = re.sub(r"\.(?! )", ". ","".join(elements_text[1:]))
                definition = definition.replace('\xa0',' ').replace('\\’', "'").replace('’',"'").replace('”','"').replace('“','"')
                mdef[metric] = [definition]

    # Format dictionary into dataframe and return dataframe
    df = pd.DataFrame(mdef)
    df = df.transpose()
    df = df.reset_index()
    df.columns = ['Metric', 'Definition']
    return df

if __name__ == "__main__":
    """ set username and password for testing scrape_kpi_matrix_page function.
    
    For proper use, I suggest looping through a config that is a list of links to kpi matrix pages.
    """
    username = None
    password = None
    if not username:
        username = input("Enter your servicetitan username:\n")
    if not password:
        import getpass
        password = getpass.getpass("Enter your servicetitan password:\n")
    scrape_kpi_matrix_page(username,password).to_excel('kpi_matrix.xlsx')
