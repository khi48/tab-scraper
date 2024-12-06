# Tab Scraper
Scrape TAB data for betting information and analyse results

* Pulls Schedule, Odds and Results every hour of each day
* Uses MongoDB to store data
* Runs scripts to analyse different betting styles

# Setup

## Mongodb Setup

`./service_setup mongodb-docker-compose.service`

## TAB Scraper Setup

`docker build -t tab_scraper .`

`./service_setup tab-scraper-docker-compose.service`

