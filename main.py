import concurrent
import concurrent.futures
import json
import requests
import threading
import xlsxwriter

from bs4 import BeautifulSoup

car_link = "https://www.edmunds.com/cars-for-sale-by-owner/"

columns = {
    'url': 'A',
    'name': 'B',
    'price': 'C',
    'vin': 'D',
    'features': 'E',
    'summary': 'F',
}

class Scrapper():
    incidence_log_file = 'incidence.json'
    def __init__(self, key='1', zip_code='45011'):
        self.initialize_scrapper()
        self.current_page = 0
        self.has_next = True
        self.incidences = []
        self.car_details_incidence = []
        self.zip_code = zip_code
        self.car_links_filename = f'carlink{key}.json'
        self.car_details_filename = f'cardetails{key}.json'
        self.xlsx_filename = f'cardetails{key}.xlsx'

    
    def initialize_scrapper(self):
        """
        Initialize the scrapper by feeding in the required headers for the scrapping to work.
        """
        session = requests.session()
        session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        session.headers['referer'] = 'https://www.edmunds.com/inventory/srp.html?inventorytype=used%2Ccpo%2Cnew&radius=25&wz=10'
        self.session = session
    
    def scrape_next_page(self):
        self.current_page += 1
        res = self.session.get(f'https://www.edmunds.com/gateway/api/purchasefunnel/v1/srp/inventory?dma=535&radius=25&zip={self.zip_code}&fetchSuggestedFacets=true&pageNum={self.current_page}')
        self.has_next = self.current_page < 555
        page_car_links = []
        for car in res.json()['inventories']['results']:
            car_info = car['vehicleInfo']['styleInfo']
            page_car_links.append(f"{car_info['make']}/{car_info['model']}/{car_info['year']}/vin/{car['vin']}")
        return page_car_links
    
    def scrape_all_links(self):
        with open(self.car_links_filename, 'w') as file:
            file.write('')
        self.current_page = 0
        while(self.has_next):
            print(f'Scrapping page: \t {self.current_page + 1} ....', end='\t')
            links = self.scrape_next_page()
            with open(self.car_links_filename, 'a') as file:
                file.write('\n'.join(links) + '\n')
            print('done.')
        with open(self.incidence_log_file, 'w') as file:
            file.write(json.dumps(self.incidences))

    def find_car_details(self, path):
        url = f'https://www.edmunds.com/{path}'
        res = self.session.get(url, timeout=20)
        bs = BeautifulSoup(res.text)
        name = bs.find('h1').text
        price = 0
        try:
            price = int(bs.find('div', {'class': 'price-summary-section'}).find('span').text.replace('$', '').replace(',', '')) #convert price to int
        except:
            pass
        vin = bs.select('div.text-gray-darker.small')[0].find('span').text.split(' ')[-1]
        car_features = []
        feature_section_el = bs.find('section', {'class': 'features-and-specs'})
        if feature_section_el:
            for feature_section in feature_section_el.find_all('ul'):
                features = feature_section.find_all('li')
                for feature in features:
                    car_features.append(feature.text)
        summaries = bs.find('section', {'class': 'vehicle-summary'}).find('div').find_all('div', {'class': 'row'})
        car_summary = {}
        for summary in summaries:
            title = None
            try:
                title = summary.find('i')['title']
            except TypeError:
                title = summary.find('span').find('span')['aria-label']
            value = summary.text.split(':')[-1]
            car_summary[title] = value
        return {
            columns['url']: url,
            columns['name']: name,
            columns['price']: price,
            columns['vin']: vin,
            columns['features']: "\n".join(car_features),
            columns['summary']: "\n".join([f'{key}: {value}' for (key, value) in car_summary.items()]),
        } # Convert keys to ABCD... so that it can be used with xlsx writer

    def report_incidence(self, path, message):
        """
        Save incidences while trying to fetch details, this will be used to resolve issues with the script.
        """
        self.incidences.append({'path': path, 'message': message})

    def save_incidences(self):
        """
        This is to save incidences in batches and reset back to zero so as not to overload the memory
        """
        with open(self.incidence_log_file, 'a') as file:
            file.writelines([json.dumps(incidence) + "\n" for incidence in self.incidences])
        self.incidences = []
    
    def save_car_info(self, data):
        """
        This is to save car details in batch and reset back to zero.
        I am saving to files first, because I can't find a way of saving to xslx without holding everything in memory.
        """
        with open(self.car_details_filename, 'a') as file:
            file.writelines([json.dumps(car) + '\n' for car in data])

    def handle_selected_lines(self, data):
        """
        This is to process car details in chunk.
        I used threadpool to improve the speed over the network as network based events usually block the cpu cycle and causes cpu cycles to be wasted.
        """
        print(f'working on {data[0][1]} - {data[-1][1]} ....', end='\t')
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.find_car_details, line[0]): line for line in data}
            for future in concurrent.futures.as_completed(future_to_url):
                params = future_to_url[future]
                try:
                    data = future.result()
                except Exception as e:
                    self.report_incidence(params, str(e))
                else:
                    results.append(data)
        self.save_car_info(results)
        self.save_incidences()
        print('.... done')

    def handle_car_details(self):
        """
        This function reads the file containing car detail links and process them in chunk.
        """
        with open(self.car_details_filename, 'w') as file:
            file.write('')
        new_items = []
        line_no = 1
        with open(self.car_links_filename, 'r') as file:
            while True:
                line = file.readline().replace('\n', '')
                if not line:
                    break
                new_items.append((line, line_no))
                if line_no % 10 == 0:
                    self.handle_selected_lines(new_items)
                    new_items = []
                line_no += 1
        self.handle_selected_lines(new_items)
    
    def save_to_excel(self):
        """
        This function reads the file containing car detial information and save them to excel.
        I have to manage with holding data to memory here, but at least, it is only for a short period of time as everything is implemented within a few seconds.
        """
        line_no = 1
  
        workbook = xlsxwriter.Workbook(self.xlsx_filename)
        worksheet = workbook.add_worksheet()
        with open(self.car_details_filename, 'r') as file:
            while True:
                line = file.readline().replace('\n', '')
                if not line:
                    break
                line_dict = json.loads(line)
                if line_dict.get('path'):
                    continue
                [worksheet.write(f'{key}{line_no}', value) for key, value in line_dict.items()]
                if line_no % 10 == 0:
                    print(f'{line_no} lines completed')
                line_no += 1
        workbook.close()
        print('done')

if __name__=='__main__':
    scraper = Scrapper()
    scraper.scrape_all_links()
    scraper.handle_car_details()
    scraper.save_to_excel()
