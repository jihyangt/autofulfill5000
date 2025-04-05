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
            "province": province,
            "extra_cold": False
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
            "province": province,
            "wednesday_date": wednesday_date,
            "thursday_date": thursday_date
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
    
    # Check for extra cold conditions (between 0°C and -2°C)
    extra_cold = False
    if wednesday_delivery and wed_avg_temp is not None and 0 >= wed_avg_temp >= -2:
        extra_cold = True
    if thursday_delivery and thu_avg_temp is not None and 0 >= thu_avg_temp >= -2:
        extra_cold = True
    
    reason = "Weather conditions acceptable for delivery"
    if not can_ship:
        reason = "Temperature too low on both Wednesday and Thursday"
    elif wednesday_delivery and not thursday_delivery:
        reason = "Delivery possible on Wednesday only"
    elif thursday_delivery and not wednesday_delivery:
        reason = "Delivery possible on Thursday only"
    
    if extra_cold:
        reason += " (Extra insulation required)"
    
    return {
        "can_ship": can_ship,
        "wednesday_delivery": wednesday_delivery,
        "thursday_delivery": thursday_delivery,
        "reason": reason,
        "wed_avg_temp": wed_avg_temp,
        "thu_avg_temp": thu_avg_temp,
        "city": city,
        "province": province,
        "extra_cold": extra_cold,
        "wednesday_date": wednesday_date,
        "thursday_date": thursday_date
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
                        # Categorize items as shrimp/potted plants or other items
                        # Special cases for specific shrimp names and excluding ShrimpsafeNet
                        is_shrimp_or_potted = (
                            ('shrimp' in item_name.lower() and not 'shrimpsafe' in item_name.lower()) or 
                            'extreme blue bolt' in item_name.lower() or
                            'red pinto galaxy' in item_name.lower() or
                            'pack' in item_name.lower() or 
                            'potted' in item_name.lower()
                            # Removed 1-2-grow as requested
                        )
                        
                        order_quantities[order_id].append({
                            'quantity': qty,
                            'item_name': item_name,
                            'is_shrimp_or_potted': is_shrimp_or_potted
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
                
                # Create categorized packing lists
                packing_list = ""
                shrimp_potted_list = []
                other_items_list = []
                
                if order_id in order_quantities:
                    for item in order_quantities[order_id]:
                        item_str = f"{item['quantity']} x {item['item_name']}"
                        if item['is_shrimp_or_potted']:
                            shrimp_potted_list.append(item_str)
                        else:
                            other_items_list.append(item_str)
                    
                    # Create combined packing list for the full order
                    all_items = []
                    if shrimp_potted_list:
                        all_items.append("SHRIMP+POTTED: " + ", ".join(shrimp_potted_list))
                    if other_items_list:
                        all_items.append("OTHER ITEMS: " + ", ".join(other_items_list))
                    packing_list = " | ".join(all_items)
                
                if city and province:
                    shipping_result = analyze_shipping_conditions(city, province)
                    shipping_result['order_id'] = order_id
                    shipping_result['customer_name'] = shipping_name
                    shipping_result['packing_list'] = packing_list.strip()
                    shipping_result['shrimp_potted_items'] = ", ".join(shrimp_potted_list) if shrimp_potted_list else ""
                    shipping_result['other_items'] = ", ".join(other_items_list) if other_items_list else ""
                    shipping_result['shipping_day'] = "Wednesday" if shipping_result['wednesday_delivery'] else "Thursday" if shipping_result['thursday_delivery'] else "None"
                    shipping_decisions.append(shipping_result)
                    processed_order_ids.add(order_id)
                else:
                    # Get current date for wednesday and thursday dates
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
                        "extra_cold": False,
                        "packing_list": packing_list.strip(),
                        "shrimp_potted_items": ", ".join(shrimp_potted_list) if shrimp_potted_list else "",
                        "other_items": ", ".join(other_items_list) if other_items_list else "",
                        "shipping_day": "None",
                        "wednesday_date": wednesday_date,
                        "thursday_date": thursday_date
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
    
    # Sort decisions by shipping day and can_ship field
    sorted_decisions = sorted(decisions, key=lambda x: (x['shipping_day'] != 'Wednesday', x['shipping_day'] != 'Thursday', not x['can_ship']))
    
    # Prepare decisions with modified fields
    modified_decisions = []
    for decision in sorted_decisions:
        
        # Determine heatpack requirement based on temperature
        needs_heatpack = "N"
        if decision['wednesday_delivery'] and decision['wed_avg_temp'] is not None and decision['wed_avg_temp'] < 8.0:
            needs_heatpack = "Y"
        elif decision['thursday_delivery'] and decision['thu_avg_temp'] is not None and decision['thu_avg_temp'] < 8.0:
            needs_heatpack = "Y"
        
        # Update shipping day to include date or set to N/A if can't ship
        if not decision['can_ship']:
            shipping_day_with_date = "N/A"
        elif decision['wednesday_delivery']:
            shipping_day_with_date = f"Wednesday {decision['wednesday_date']}"
        elif decision['thursday_delivery']:
            shipping_day_with_date = f"Thursday {decision['thursday_date']}"
        else:
            shipping_day_with_date = "N/A"
        
        # Create modified decision with only required fields
        # Add temperature for the shipping day
        shipping_temp = None
        if decision['wednesday_delivery'] and decision['shipping_day'] == 'Wednesday':
            shipping_temp = decision['wed_avg_temp']
        elif decision['thursday_delivery'] and decision['shipping_day'] == 'Thursday':
            shipping_temp = decision['thu_avg_temp']
            
        # Format temperature to 1 decimal place if available
        formatted_temp = f"{shipping_temp:.1f}°C" if shipping_temp is not None else "N/A"
        
        modified_decision = {
            'order_id': decision['order_id'],
            'customer_name': decision['customer_name'],
            'city': decision['city'],
            'province': decision['province'],
            'can_ship': decision['can_ship'],
            'shipping_day': shipping_day_with_date,
            'temperature': formatted_temp,
            'extra_cold': decision['extra_cold'],
            'heatpack': needs_heatpack,
            'packing_list': decision['packing_list']
        }
        modified_decisions.append(modified_decision)
    
    # Define new fieldnames without the removed fields
    fieldnames = ['order_id', 'customer_name', 'city', 'province', 'can_ship', 'shipping_day', 'temperature', 'extra_cold', 'heatpack', 'packing_list']
    
    with open(output_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for decision in modified_decisions:
            writer.writerow(decision)
    
    print(f"Shipping report generated: {output_file}")
    
    # Generate warehouse pick list for 'other items' grouped by shipping day
    generate_warehouse_pick_list(decisions)
    
    # Print summary
    total = len(decisions)
    shippable = sum(1 for d in decisions if d['can_ship'])
    extra_cold = sum(1 for d in decisions if d['extra_cold'])
    wednesday_orders = sum(1 for d in decisions if d['shipping_day'] == 'Wednesday' and d['can_ship'])
    thursday_orders = sum(1 for d in decisions if d['shipping_day'] == 'Thursday' and d['can_ship'])
    
    print(f"\nSummary:")
    print(f"Total orders: {total}")
    print(f"Shippable orders: {shippable} ({shippable/total*100:.1f}%)")
    print(f"  - Wednesday deliveries: {wednesday_orders}")
    print(f"  - Thursday deliveries: {thursday_orders}")
    print(f"  - Extra cold orders (requiring insulation): {extra_cold}")
    print(f"Unshippable orders: {total-shippable} ({(total-shippable)/total*100:.1f}%)")


def generate_warehouse_pick_list(decisions, output_file="warehouse_pick_list.csv"):
    """Generate a consolidated warehouse pick list for 'other items' grouped by shipping day"""
    if not decisions:
        print("No shipping decisions to report.")
        return
    
    # Filter for shippable orders only
    shippable_orders = [d for d in decisions if d['can_ship']]
    
    # Group by shipping day
    wednesday_orders = [d for d in shippable_orders if d['shipping_day'] == 'Wednesday']
    thursday_orders = [d for d in shippable_orders if d['shipping_day'] == 'Thursday']
    
    # Create a dictionary to track item quantities by day
    warehouse_items = {
        'Wednesday': {},
        'Thursday': {}
    }
    
    # Process Wednesday orders
    for order in wednesday_orders:
        if not order['other_items']:
            continue
            
        # Split the other_items string into individual items
        items = order['other_items'].split(', ')
        for item in items:
            # Extract quantity and item name
            parts = item.split(' x ', 1)
            if len(parts) == 2:
                qty_str, item_name = parts
                try:
                    qty = int(qty_str)
                    if item_name in warehouse_items['Wednesday']:
                        warehouse_items['Wednesday'][item_name] += qty
                    else:
                        warehouse_items['Wednesday'][item_name] = qty
                except ValueError:
                    # Skip if quantity is not a valid number
                    pass
    
    # Process Thursday orders
    for order in thursday_orders:
        if not order['other_items']:
            continue
            
        # Split the other_items string into individual items
        items = order['other_items'].split(', ')
        for item in items:
            # Extract quantity and item name
            parts = item.split(' x ', 1)
            if len(parts) == 2:
                qty_str, item_name = parts
                try:
                    qty = int(qty_str)
                    if item_name in warehouse_items['Thursday']:
                        warehouse_items['Thursday'][item_name] += qty
                    else:
                        warehouse_items['Thursday'][item_name] = qty
                except ValueError:
                    # Skip if quantity is not a valid number
                    pass
    
    # Write to CSV
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Shipping Day', 'Item', 'Total Quantity'])
        
        # Write Wednesday items
        if warehouse_items['Wednesday']:
            for item_name, qty in sorted(warehouse_items['Wednesday'].items()):
                writer.writerow(['Wednesday', item_name, qty])
        
        # Write Thursday items
        if warehouse_items['Thursday']:
            for item_name, qty in sorted(warehouse_items['Thursday'].items()):
                writer.writerow(['Thursday', item_name, qty])
    
    print(f"Warehouse pick list generated: {output_file}")


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
