#!/usr/bin/env python3

import requests
import csv
import datetime
from datetime import datetime, timedelta
import json
from collections import defaultdict, Counter
from config import SHOPIFY_SHOP_URL, SHOPIFY_ACCESS_TOKEN

# Shopify API configuration
API_VERSION = '2023-10'
HEADERS = {
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}
BASE_URL = f"https://{SHOPIFY_SHOP_URL}/admin/api/{API_VERSION}"


def get_coordinates(city, province, country="Canada"):
    """Get latitude and longitude coordinates for a location"""
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


def get_shopify_orders():
    """Fetch orders from Shopify API"""
    orders = []
    # Get orders from the last 30 days to ensure we have enough data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}/orders.json?status=any&limit=250&created_at_min={start_date_str}"
    print(f"Fetching orders from Shopify API: {url}")
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error fetching orders: {response.status_code}")
            print(response.text)
            return []
        
        data = response.json()
        if 'orders' in data:
            print(f"Found {len(data['orders'])} orders in current page")
            orders.extend(data['orders'])
        else:
            print("No orders found in API response")
            print(f"Response keys: {data.keys()}")
            break
        
        # Check for pagination
        link_header = response.headers.get('Link')
        url = None
        if link_header and 'rel="next"' in link_header:
            next_link = [l for l in link_header.split(',') if 'rel="next"' in l]
            if next_link:
                url = next_link[0].split('<')[1].split('>')[0]
                print(f"Fetching next page: {url}")
    
    print(f"Total orders fetched: {len(orders)}")
    return orders


def categorize_line_item(item_name):
    """Categorize line item as either 'potted_shrimp' or 'other'"""
    item_name_lower = item_name.lower()
    if 'potted' in item_name_lower or 'shrimp' in item_name_lower:
        return 'potted_shrimp'
    else:
        return 'other'


def process_orders_csv_fallback(csv_file="orders_export (4).csv"):
    """Process orders from CSV as a fallback when Shopify API fails"""
    shipping_decisions = []
    
    try:
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            
            # Track processed order IDs to avoid duplicates (since the CSV has multiple rows per order)
            processed_order_ids = set()
            # Track quantities for each order
            order_quantities = {}
            order_items_by_category = {}
            
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
                        order_items_by_category[order_id] = {'potted_shrimp': [], 'other': []}
                    
                    try:
                        qty = int(quantity)
                        item_info = {
                            'quantity': qty,
                            'item_name': item_name
                        }
                        order_quantities[order_id].append(item_info)
                        
                        # Categorize the item
                        category = categorize_line_item(item_name)
                        item_str = f"{qty} x {item_name}"
                        order_items_by_category[order_id][category].append(item_str)
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
                
                if city and province:
                    shipping_result = analyze_shipping_conditions(city, province)
                    shipping_result['order_id'] = order_id
                    shipping_result['customer_name'] = shipping_name
                    
                    # Add categorized items
                    if order_id in order_items_by_category:
                        shipping_result['potted_shrimp_items'] = ", ".join(order_items_by_category[order_id]['potted_shrimp'])
                        shipping_result['other_items'] = ", ".join(order_items_by_category[order_id]['other'])
                        all_items = order_items_by_category[order_id]['potted_shrimp'] + order_items_by_category[order_id]['other']
                        shipping_result['all_items'] = ", ".join(all_items)
                    else:
                        shipping_result['potted_shrimp_items'] = ""
                        shipping_result['other_items'] = ""
                        shipping_result['all_items'] = ""
                    
                    shipping_decisions.append(shipping_result)
                    processed_order_ids.add(order_id)
                else:
                    print(f"Order {order_id} has incomplete shipping information, skipping.")
    
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
        return []
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        return []
    
    return shipping_decisions


def process_shopify_orders():
    """Process orders from Shopify API and determine shipping eligibility"""
    shopify_orders = get_shopify_orders()
    shipping_decisions = []
    
    print(f"Processing {len(shopify_orders)} orders from Shopify")
    
    # If no orders found, try to use the CSV file as fallback for testing
    if not shopify_orders:
        print("No orders found from Shopify API, falling back to CSV file for testing")
        return process_orders_csv_fallback()
    
    # For testing purposes, always use CSV fallback for now
    print("Using CSV fallback for testing purposes")
    return process_orders_csv_fallback()
    
    # Process each order
    for order in shopify_orders:
        order_id = order.get('name', '').replace('#', '')
        shipping_address = order.get('shipping_address', {})
        
        # Skip orders without shipping address
        if not shipping_address:
            print(f"Order {order_id} has no shipping address, skipping.")
            continue
            
        city = shipping_address.get('city', '')
        province = shipping_address.get('province', '')
        shipping_name = shipping_address.get('name', '')
        
        # Skip orders without shipping information
        if not city or not province:
            print(f"Order {order_id} has incomplete shipping information, skipping.")
            continue
        
        # Process line items
        line_items = order.get('line_items', [])
        potted_shrimp_items = []
        other_items = []
        
        for item in line_items:
            item_name = item.get('name', '')
            quantity = item.get('quantity', 0)
            
            if quantity > 0:
                item_str = f"{quantity} x {item_name}"
                category = categorize_line_item(item_name)
                
                if category == 'potted_shrimp':
                    potted_shrimp_items.append(item_str)
                else:
                    other_items.append(item_str)
        
        # Get shipping conditions
        shipping_result = analyze_shipping_conditions(city, province)
        
        # Add order details to shipping result
        shipping_result['order_id'] = order_id
        shipping_result['customer_name'] = shipping_name
        shipping_result['potted_shrimp_items'] = ", ".join(potted_shrimp_items) if potted_shrimp_items else ""
        shipping_result['other_items'] = ", ".join(other_items) if other_items else ""
        shipping_result['all_items'] = ", ".join(potted_shrimp_items + other_items)
        
        # Add to shipping decisions
        shipping_decisions.append(shipping_result)
    
    return shipping_decisions


def generate_shipping_report(decisions, output_file="shopify_shipping_decisions.csv"):
    """Generate a CSV report of shipping decisions with categorized packing list"""
    if not decisions:
        print("No shipping decisions to report.")
        return
    
    # First, sort by delivery day (Wednesday first, then Thursday)
    def sort_key(decision):
        # First sort by can_ship (True first)
        can_ship_key = 0 if decision['can_ship'] else 1
        
        # Then sort by delivery day (Wednesday first, then Thursday)
        if decision['wednesday_delivery']:
            day_key = 0
        elif decision['thursday_delivery']:
            day_key = 1
        else:
            day_key = 2
            
        return (can_ship_key, day_key)
    
    sorted_decisions = sorted(decisions, key=sort_key)
    
    # Define CSV fields
    fieldnames = [
        'order_id', 'customer_name', 'city', 'province', 
        'can_ship', 'wednesday_delivery', 'thursday_delivery', 
        'reason', 'wed_avg_temp', 'thu_avg_temp', 
        'potted_shrimp_items', 'other_items', 'all_items'
    ]
    
    # Write to CSV
    with open(output_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for decision in sorted_decisions:
            writer.writerow(decision)
    
    print(f"Shipping report generated: {output_file}")
    
    # Generate summary statistics
    total = len(decisions)
    shippable = sum(1 for d in decisions if d['can_ship'])
    wednesday_deliveries = sum(1 for d in decisions if d['can_ship'] and d['wednesday_delivery'])
    thursday_deliveries = sum(1 for d in decisions if d['can_ship'] and d['thursday_delivery'])
    
    print(f"\nSummary:")
    print(f"Total orders: {total}")
    print(f"Shippable orders: {shippable} ({shippable/total*100:.1f}% of total)")
    print(f"  - Wednesday deliveries: {wednesday_deliveries}")
    print(f"  - Thursday deliveries: {thursday_deliveries}")
    print(f"Unshippable orders: {total-shippable} ({(total-shippable)/total*100:.1f}% of total)")
    
    # Generate 'others' products summary
    generate_others_summary(decisions)


def generate_others_summary(decisions):
    """Generate a summary of all 'other' products for warehouse collection"""
    other_products = []
    
    # Extract all 'other' items from decisions
    for decision in decisions:
        if decision['can_ship'] and decision['other_items']:
            items = decision['other_items'].split(", ")
            other_products.extend(items)
    
    if not other_products:
        print("\nNo 'other' products to summarize.")
        return
    
    # Parse quantities and names
    product_counts = defaultdict(int)
    for item in other_products:
        parts = item.split(" x ", 1)
        if len(parts) == 2:
            try:
                quantity = int(parts[0])
                product_name = parts[1]
                product_counts[product_name] += quantity
            except ValueError:
                # If quantity can't be parsed, just count it as 1
                product_counts[item] += 1
        else:
            # If format is unexpected, just count it as 1
            product_counts[item] += 1
    
    # Sort by quantity (highest first)
    sorted_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Write summary to CSV
    with open("other_products_summary.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Product Name", "Total Quantity"])
        for product, count in sorted_products:
            writer.writerow([product, count])
    
    print("\nOther Products Summary:")
    print("------------------------")
    for product, count in sorted_products:
        print(f"{count} x {product}")
    print("\nSummary saved to: other_products_summary.csv")


# Main execution
if __name__ == "__main__":
    # Process orders and generate shipping report
    shipping_decisions = process_shopify_orders()
    generate_shipping_report(shipping_decisions)