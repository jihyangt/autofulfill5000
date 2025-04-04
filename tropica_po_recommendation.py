#!/usr/bin/env python3

import os
import sys
import csv
import json
import requests
from datetime import datetime, timedelta
from tabulate import tabulate
from config import SHOPIFY_SHOP_URL, SHOPIFY_ACCESS_TOKEN

# Shopify API configuration
API_VERSION = '2023-10'
HEADERS = {
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}
BASE_URL = f"https://{SHOPIFY_SHOP_URL}/admin/api/{API_VERSION}"

# Constants for the recommender system
VENDOR_NAME = "Tropica"  # Only recommend products from this vendor
ORDER_FREQUENCY_DAYS = 14  # Ordering every 2 weeks
MIN_STOCK_THRESHOLD = 5  # Minimum stock to maintain
RESTOCK_BUFFER = 1.2  # Buffer multiplier for recommended quantity (20% extra)


def get_products(vendor=None):
    """Fetch products from Shopify, optionally filtered by vendor"""
    products = []
    url = f"{BASE_URL}/products.json?limit=250"
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error fetching products: {response.status_code}")
            print(response.text)
            return []
        
        data = response.json()
        
        # Filter by vendor if specified
        if vendor:
            products.extend([p for p in data['products'] if p.get('vendor') == vendor])
        else:
            products.extend(data['products'])
        
        # Check for pagination
        link_header = response.headers.get('Link')
        url = None
        if link_header and 'rel="next"' in link_header:
            # Extract next URL from Link header
            next_link = [l for l in link_header.split(',') if 'rel="next"' in l]
            if next_link:
                url = next_link[0].split('<')[1].split('>')[0]
    
    return products


def get_inventory_levels(product_ids):
    """Get inventory levels for the given product IDs"""
    inventory_items = {}
    inventory_levels = {}
    
    # First, get inventory item IDs for all variants
    for product_id in product_ids:
        url = f"{BASE_URL}/products/{product_id}/variants.json"
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error fetching variants for product {product_id}: {response.status_code}")
            continue
        
        variants = response.json().get('variants', [])
        for variant in variants:
            inventory_items[variant['id']] = {
                'inventory_item_id': variant['inventory_item_id'],
                'product_id': product_id,
                'title': variant.get('title')
            }
    
    # Now get inventory levels for all inventory items
    inventory_item_ids = [item['inventory_item_id'] for item in inventory_items.values()]
    
    # Process in batches of 50 to avoid URL length limits
    batch_size = 50
    for i in range(0, len(inventory_item_ids), batch_size):
        batch = inventory_item_ids[i:i+batch_size]
        ids_param = ','.join(str(id) for id in batch)
        url = f"{BASE_URL}/inventory_levels.json?inventory_item_ids={ids_param}"
        
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error fetching inventory levels: {response.status_code}")
            continue
        
        levels = response.json().get('inventory_levels', [])
        for level in levels:
            inventory_levels[level['inventory_item_id']] = level['available']
    
    # Map inventory levels back to variants and products
    result = {}
    for variant_id, item in inventory_items.items():
        product_id = item['product_id']
        if product_id not in result:
            result[product_id] = 0
        
        # Add inventory for this variant to the product total
        inventory_item_id = item['inventory_item_id']
        if inventory_item_id in inventory_levels:
            result[product_id] += inventory_levels[inventory_item_id]
    
    return result


def get_incoming_inventory(product_ids):
    """Get incoming inventory from open purchase orders"""
    # This is a simplified version - in a real implementation, you would
    # query your purchase order system or use Shopify's draft order API
    # For now, we'll return 0 for all products
    return {product_id: 0 for product_id in product_ids}


def get_recent_orders(days=14):
    """Get orders from the last specified number of days"""
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format dates for Shopify API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    orders = []
    url = f"{BASE_URL}/orders.json?status=any&created_at_min={start_date_str}&created_at_max={end_date_str}&limit=250"
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error fetching orders: {response.status_code}")
            print(response.text)
            return []
        
        data = response.json()
        orders.extend(data['orders'])
        
        # Check for pagination
        link_header = response.headers.get('Link')
        url = None
        if link_header and 'rel="next"' in link_header:
            next_link = [l for l in link_header.split(',') if 'rel="next"' in l]
            if next_link:
                url = next_link[0].split('<')[1].split('>')[0]
    
    return orders


def calculate_sales_by_product(orders, product_map):
    """Calculate total sales quantity for each product in the given orders"""
    sales_by_product = {}
    
    for order in orders:
        # Skip cancelled or refunded orders
        if order.get('cancelled_at') or order.get('refunded_at'):
            continue
            
        for line_item in order.get('line_items', []):
            product_id = line_item.get('product_id')
            if not product_id or product_id not in product_map:
                continue
                
            if product_id not in sales_by_product:
                sales_by_product[product_id] = 0
                
            sales_by_product[product_id] += line_item.get('quantity', 0)
    
    return sales_by_product


def calculate_recommended_quantity(sales_quantity, current_inventory, incoming_inventory):
    """Calculate recommended order quantity based on sales, current inventory, and incoming inventory"""
    # Expected sales for the next order period
    expected_sales = sales_quantity
    
    # Total available inventory (current + incoming)
    total_available = current_inventory + incoming_inventory
    
    # Calculate how much we need
    needed = max(0, (expected_sales * RESTOCK_BUFFER) - total_available)
    
    # Ensure we maintain minimum stock levels
    if total_available < MIN_STOCK_THRESHOLD:
        needed += (MIN_STOCK_THRESHOLD - total_available)
    
    return round(needed)


def generate_purchase_order_recommendations():
    """Generate purchase order recommendations for Tropica plant products"""
    print(f"Fetching products from vendor: {VENDOR_NAME}...")
    products = get_products(vendor=VENDOR_NAME)
    
    if not products:
        print(f"No products found for vendor: {VENDOR_NAME}")
        return
    
    print(f"Found {len(products)} products from {VENDOR_NAME}")
    
    # Create a map of product IDs to product details
    product_map = {p['id']: p for p in products}
    product_ids = list(product_map.keys())
    
    # Get current inventory levels
    print("Fetching current inventory levels...")
    inventory_levels = get_inventory_levels(product_ids)
    
    # Get incoming inventory
    print("Fetching incoming inventory...")
    incoming_inventory = get_incoming_inventory(product_ids)
    
    # Get recent orders
    print(f"Fetching orders from the last {ORDER_FREQUENCY_DAYS} days...")
    recent_orders = get_recent_orders(days=ORDER_FREQUENCY_DAYS)
    
    # Calculate sales by product
    sales_by_product = calculate_sales_by_product(recent_orders, product_map)
    
    # Generate recommendations
    recommendations = []
    for product_id, product in product_map.items():
        product_title = product.get('title', 'Unknown Product')
        sales_quantity = sales_by_product.get(product_id, 0)
        current_inventory = inventory_levels.get(product_id, 0)
        incoming = incoming_inventory.get(product_id, 0)
        
        recommended_quantity = calculate_recommended_quantity(
            sales_quantity, current_inventory, incoming
        )
        
        recommendations.append({
            'product_id': product_id,
            'item': product_title,
            'sales_last_2_weeks': sales_quantity,
            'current_inventory': current_inventory,
            'incoming_inventory': incoming,
            'recommended_order': recommended_quantity
        })
    
    # Sort recommendations by recommended order quantity (descending)
    recommendations.sort(key=lambda x: x['recommended_order'], reverse=True)
    
    return recommendations


def display_recommendations(recommendations):
    """Display recommendations in a formatted table"""
    if not recommendations:
        print("No recommendations available.")
        return
    
    # Prepare table data
    table_data = []
    for rec in recommendations:
        table_data.append([
            rec['item'],
            rec['sales_last_2_weeks'],
            rec['current_inventory'],
            rec['incoming_inventory'],
            rec['recommended_order']
        ])
    
    # Display table
    headers = ["Item", "Sales (2 Weeks)", "Current Inventory", "Incoming", "Recommended Order"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Export to CSV
    export_to_csv(recommendations)


def export_to_csv(recommendations, filename="tropica_order_recommendations.csv"):
    """Export recommendations to a CSV file"""
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['item', 'sales_last_2_weeks', 'current_inventory', 
                     'incoming_inventory', 'recommended_order']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        
        writer.writeheader()
        for rec in recommendations:
            writer.writerow(rec)
    
    print(f"\nRecommendations exported to {filename}")


def main():
    """Main function to run the purchase order recommender"""
    print("\n===== Tropica Plant Purchase Order Recommender =====\n")
    
    try:
        recommendations = generate_purchase_order_recommendations()
        if recommendations:
            display_recommendations(recommendations)
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())