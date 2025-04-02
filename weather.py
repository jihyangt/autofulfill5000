import requests
import csv
import datetime
from datetime import datetime, timedelta


def get_coordinates(city, province, country="Canada"):
    query = f"{city}, {province}, {country}"
    geo_url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
    response = requests.get(geo_url, headers={"User-Agent": "Mozilla/5.0"})

    if response.status_code == 200 and response.json():
        location = response.json()[0]
        return float(location["lat"]), float(location["lon"])
    else:
        print(f"Error fetching coordinates or location not found for {city}, {province}.")
        return None, None


def get_weather_forecast(latitude, longitude, start_date=None, end_date=None):
    """Get hourly weather forecast for a date range"""
    # If no dates provided, default to today through end of week
    if start_date is None or end_date is None:
        today = datetime.now()
        # Calculate days until next Wednesday and Thursday
        days_until_end_of_week = 7 - today.weekday() + 4  # End of next week (Friday)
        start_date = today.strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=days_until_end_of_week)).strftime("%Y-%m-%d")
    
    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m&start_date={start_date}&end_date={end_date}"
    response = requests.get(weather_url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching weather forecast data")
        return None


def analyze_shipping_conditions(city, province):
    """Analyze if shipping is possible based on temperature conditions for Wednesday and Thursday"""
    latitude, longitude = get_coordinates(city, province)
    if latitude is None or longitude is None:
        return {
            "can_ship": False,
            "wednesday_delivery": False,
            "thursday_delivery": False,
            "reason": "Location not found",
            "wed_avg_temp": None,
            "thu_avg_temp": None,
            "city": city,
            "province": province
        }

    # Get current date and calculate the dates for next Wednesday and Thursday
    today = datetime.now()
    days_until_wednesday = (2 - today.weekday()) % 7  # 2 represents Wednesday
    days_until_thursday = (3 - today.weekday()) % 7   # 3 represents Thursday
    
    # If today is after Wednesday/Thursday, get next week's dates
    if days_until_wednesday == 0 and today.hour >= 17:  # After 5pm on Wednesday
        days_until_wednesday = 7
    if days_until_thursday == 0 and today.hour >= 17:  # After 5pm on Thursday
        days_until_thursday = 7
        
    wednesday_date = (today + timedelta(days=days_until_wednesday)).strftime("%Y-%m-%d")
    thursday_date = (today + timedelta(days=days_until_thursday)).strftime("%Y-%m-%d")
    
    # Get forecast from today until end of next week to ensure we have data
    forecast_data = get_weather_forecast(latitude, longitude)
    if not forecast_data:
        return {
            "can_ship": False,
            "wednesday_delivery": False,
            "thursday_delivery": False,
            "reason": "Weather data unavailable",
            "wed_avg_temp": None,
            "thu_avg_temp": None,
            "city": city,
            "province": province
        }
    
    # Extract hourly data
    hourly_times = forecast_data.get("hourly", {}).get("time", [])
    hourly_temps = forecast_data.get("hourly", {}).get("temperature_2m", [])
    
    # Filter for Wednesday's data between 10am and 5pm
    wednesday_business_hours_temps = []
    thursday_business_hours_temps = []
    
    for i, time_str in enumerate(hourly_times):
        date_part = time_str.split("T")[0]
        hour = int(time_str.split("T")[1].split(":")[0])
        
        if date_part == wednesday_date and 10 <= hour <= 17:  # 10am to 5pm on Wednesday
            wednesday_business_hours_temps.append(hourly_temps[i])
        elif date_part == thursday_date and 10 <= hour <= 17:  # 10am to 5pm on Thursday
            thursday_business_hours_temps.append(hourly_temps[i])
    
    # Calculate average temperatures and determine shipping eligibility
    wed_avg_temp = None
    thu_avg_temp = None
    wednesday_delivery = False
    thursday_delivery = False
    
    if wednesday_business_hours_temps:
        wed_avg_temp = sum(wednesday_business_hours_temps) / len(wednesday_business_hours_temps)
        wednesday_delivery = wed_avg_temp >= -2.0
    
    if thursday_business_hours_temps:
        thu_avg_temp = sum(thursday_business_hours_temps) / len(thursday_business_hours_temps)
        thursday_delivery = thu_avg_temp >= -2.0
    
    # Determine overall shipping possibility
    can_ship = wednesday_delivery or thursday_delivery
    
    reason = "Weather conditions acceptable for delivery"
    if not can_ship:
        reason = "Temperature too low on both Wednesday and Thursday"
    elif wednesday_delivery and not thursday_delivery:
        reason = "Delivery possible on Wednesday only"
    elif thursday_delivery and not wednesday_delivery:
        reason = "Delivery possible on Thursday only"
    
    return {
        "can_ship": can_ship,
        "wednesday_delivery": wednesday_delivery,
        "thursday_delivery": thursday_delivery,
        "reason": reason,
        "wed_avg_temp": wed_avg_temp,
        "thu_avg_temp": thu_avg_temp,
        "city": city,
        "province": province
    }


def process_orders_csv(csv_file="orders_export (4).csv"):
    """Process orders from CSV and determine shipping eligibility"""
    shipping_decisions = []
    
    try:
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            
            # Track processed order IDs to avoid duplicates (since the CSV has multiple rows per order)
            processed_order_ids = set()
            # Track quantities for each order
            order_quantities = {}
            
            # First pass: collect all items and quantities for each order
            for row in reader:
                # Extract order ID (removing the # symbol if present)
                order_id = row.get('Name', '')
                if order_id.startswith('#'):
                    order_id = order_id[1:]
                
                # Get quantity and item name
                quantity = row.get('Lineitem quantity', '')
                item_name = row.get('Lineitem name', '')
                
                if order_id and quantity and item_name:
                    if order_id not in order_quantities:
                        order_quantities[order_id] = []
                    
                    try:
                        qty = int(quantity)
                        order_quantities[order_id].append({
                            'quantity': qty,
                            'item_name': item_name
                        })
                    except ValueError:
                        # Skip if quantity is not a valid number
                        pass
            
            # Reset file pointer to beginning for second pass
            file.seek(0)
            next(reader)  # Skip header row
            
            # Second pass: process shipping decisions
            for row in reader:
                # Extract order ID (removing the # symbol if present)
                order_id = row.get('Name', '')
                if order_id.startswith('#'):
                    order_id = order_id[1:]
                
                # Skip if we've already processed this order
                if order_id in processed_order_ids:
                    continue
                
                # Extract city and province from CSV - using Shipping fields
                city = row.get('Shipping City', '')
                province = row.get('Shipping Province', '')
                shipping_name = row.get('Shipping Name', '')
                
                # Create packing list string
                packing_list = ""
                if order_id in order_quantities:
                    items = []
                    for item in order_quantities[order_id]:
                        items.append(f"{item['quantity']} x {item['item_name']}")
                    packing_list = ", ".join(items)
                
                if city and province:
                    shipping_result = analyze_shipping_conditions(city, province)
                    shipping_result['order_id'] = order_id
                    shipping_result['customer_name'] = shipping_name
                    shipping_result['packing_list'] = packing_list.strip()
                    shipping_decisions.append(shipping_result)
                    processed_order_ids.add(order_id)
                else:
                    shipping_decisions.append({
                        "order_id": order_id,
                        "customer_name": shipping_name,
                        "can_ship": False,
                        "wednesday_delivery": False,
                        "thursday_delivery": False,
                        "reason": "Missing location data",
                        "wed_avg_temp": None,
                        "thu_avg_temp": None,
                        "city": city,
                        "province": province,
                        "packing_list": packing_list.strip()
                    })
                    processed_order_ids.add(order_id)
    
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
        return []
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        return []
    
    return shipping_decisions


def generate_shipping_report(decisions, output_file="shipping_decisions.csv"):
    """Generate a CSV report of shipping decisions with packing list"""
    if not decisions:
        print("No shipping decisions to report.")
        return
    
    # Sort decisions by can_ship field (True first, False last)
    sorted_decisions = sorted(decisions, key=lambda x: not x['can_ship'])
    
    fieldnames = ['order_id', 'customer_name', 'city', 'province', 'can_ship', 'wednesday_delivery', 'thursday_delivery', 'reason', 'wed_avg_temp', 'thu_avg_temp', 'packing_list']
    
    with open(output_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for decision in sorted_decisions:
            writer.writerow(decision)
    
    print(f"Shipping report generated: {output_file}")
    
    # Print summary
    total = len(decisions)
    shippable = sum(1 for d in decisions if d['can_ship'])
    print(f"\nSummary:")
    print(f"Total orders: {total}")
    print(f"Shippable orders: {shippable} ({shippable/total*100:.1f}%)")
    print(f"Unshippable orders: {total-shippable} ({(total-shippable)/total*100:.1f}%)")


def get_weather(city, province):
    latitude, longitude = get_coordinates(city, province)
    if latitude is None or longitude is None:
        return

    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    response = requests.get(weather_url)

    if response.status_code == 200:
        data = response.json()
        weather = data.get("current_weather", {})
        print(f"Weather for {city}, {province}:")
        print(f"Temperature: {weather.get('temperature', 'N/A')}°C")
        print(f"Wind Speed: {weather.get('windspeed', 'N/A')} km/h")
        print(f"Weather Code: {weather.get('weathercode', 'N/A')}")
    else:
        print("Error fetching weather data")


# Main execution
if __name__ == "__main__":
    # Process orders and generate shipping report
    shipping_decisions = process_orders_csv()
    generate_shipping_report(shipping_decisions)
    
    # Example of individual city check
    # get_weather("Grande Prairie", "AB")
