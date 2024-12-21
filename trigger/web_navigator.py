from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pickle
import os
import json
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException
)
from selenium.webdriver.chrome.service import Service

class SessionManager:
    def __init__(self, headless=True, profile_dir: str = "selenium_profile"):
        """
        Initialize SessionManager with profile directory.
        
        Args:
            profile_dir (str): Directory to store browser profiles
        """
        self.profile_dir = profile_dir
        self.cookies_file = os.path.join(profile_dir, "cookies.pkl")
        self.headless = headless
        
        # Create profile directory if it doesn't exist
        Path(profile_dir).mkdir(parents=True, exist_ok=True)

    def create_browser_with_profile(self) -> webdriver.Chrome:
        """
        Create a Chrome browser instance with a saved profile.
        
        Returns:
            webdriver.Chrome: Browser instance with profile loaded
        """
        options = webdriver.ChromeOptions()
        options.add_argument(f"user-data-dir={self.profile_dir}")
        
        # Additional options for stability
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--user-data-dir=chrome-data")

        if self.headless:
            options.add_argument("headless")

        service = Service('/usr/bin/chromedriver')
        
        return webdriver.Chrome(options=options, service=service)

    def save_cookies(self, driver: webdriver.Chrome):
        """
        Save cookies from current session.
        
        Args:
            driver: Current WebDriver instance
        """
        cookies = driver.get_cookies()
        with open(self.cookies_file, "wb") as f:
            pickle.dump(cookies, f)

    def load_cookies(self, driver: webdriver.Chrome):
        """
        Load saved cookies into current session.
        
        Args:
            driver: Current WebDriver instance
        """
        if os.path.exists(self.cookies_file):
            with open(self.cookies_file, "rb") as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    try:
                        driver.add_cookie(cookie)
                    except Exception as e:
                        print(f"Error loading cookie: {e}")

def example_login_flow(url: str, username: str, password: str):
    """
    Example of how to use SessionManager for login persistence.
    
    Args:
        url (str): Website URL
        username (str): Login username
        password (str): Login password
    """
    # Initialize session manager
    session_mgr = SessionManager(headless=False)
    
    # Create browser with profile
    driver = session_mgr.create_browser_with_profile()
    
    try:

        # Navigate to the website
        driver.get(url)

        # # Try to load existing cookies
        # session_mgr.load_cookies(driver)
        
        # # Refresh page to apply cookies
        # driver.refresh()
        
        login_button = driver.find_element(By.XPATH, "//*[@data-testid='header-login']")


        # Check if we need to login (you'll need to implement this based on the website)
        if login_button:
            login_button.click()
            elem = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "username"))
                )

            # Perform login
            username_field = driver.find_element(By.ID, "username")  # Replace with actual ID
            password_field = driver.find_element(By.ID, "password")  # Replace with actual ID
            
            username_field.send_keys(username)
            password_field.send_keys(password)
            
            # Find and click login button
            login_button = driver.find_element(By.ID, "accept")  # Replace with actual ID
            login_button.click()

            elem = WebDriverWait(driver, 120).until(
                    EC.presence_of_element_located((By.XPATH, "//*[@data-testid='brand-bar-account']"))
                )
            
            # Save cookies after successful login
            session_mgr.save_cookies(driver)
        
        # Your automation code here...
        
    finally:
        driver.quit()

# Example usage
if __name__ == "__main__":
    # Replace with actual values
    example_login_flow(
        url="https://www.tab.co.nz/racing/pakenham/0c5d7d31-b7fa-437d-8986-4129be69eb3b",
        username="khi48",
        password="OptimalPa33word!"
    )