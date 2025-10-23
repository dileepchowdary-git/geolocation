import psycopg2
import requests
import time
import os
from dotenv import load_dotenv # New Import
from typing import Optional, Dict, List

# Load environment variables from .env file
load_dotenv()

# --- Configuration loaded from .env ---

# Database configuration
PG_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT'),  # Note: os.getenv returns strings, which psycopg2 handles
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'database': os.getenv('PG_DATABASE')
}

# Google Geocoding API key
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GEOCODING_URL = 'https://maps.googleapis.com/maps/api/geocode/json'

# ----------------------------------------
# All functions remain exactly the same, but are included below for completeness.
# ----------------------------------------

def get_leads_without_geolocation(conn) -> List[Dict]:
    """Fetch leads that don't have geolocation data"""
    query = """
        WITH ct AS (
            SELECT * FROM geolocation g WHERE type = 'lead'
        ),
        main AS (
            SELECT l.* FROM ct LEFT JOIN public.lead l ON l.id = ct.id WHERE l.stage IS NOT NULL
        )
        SELECT l.id, l.lead_name, l.address, l.pincode, l.state, l.city, l.stage
        FROM lead l
        WHERE stage IS NOT NULL 
        AND id NOT IN (SELECT id FROM main)
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        return [dict(zip(columns, row)) for row in results]


def build_address_string(lead: Dict) -> str:
    """Build a complete address string from lead data"""
    address_parts = []
    
    if lead.get('address'):
        address_parts.append(lead['address'])
    if lead.get('city'):
        address_parts.append(lead['city'])
    if lead.get('state'):
        address_parts.append(lead['state'])
    if lead.get('pincode'):
        address_parts.append(str(lead['pincode']))
    
    address_parts.append('India')
    
    return ', '.join(address_parts)


def get_geolocation(address: str) -> Optional[Dict]:
    """Fetch geolocation data from Google Geocoding API"""
    try:
        params = {
            'address': address,
            'key': GOOGLE_API_KEY  # Uses the variable loaded from .env
        }
        
        response = requests.get(GEOCODING_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            formatted_address = data['results'][0]['formatted_address']
            
            return {
                'latitude': location['lat'],
                'longitude': location['lng'],
                'formatted_address': formatted_address,
                'place_id': data['results'][0].get('place_id', '')
            }
        else:
            return {
                'error': data.get('status', 'UNKNOWN'),
                'message': data.get('error_message', 'No results found')
            }
            
    except requests.exceptions.RequestException as e:
        return {
            'error': 'API_REQUEST_FAILED',
            'message': str(e)
        }
    except Exception as e:
        return {
            'error': 'UNEXPECTED_ERROR',
            'message': str(e)
        }


def save_geolocation(conn, lead_id: int, geo_data: Dict) -> bool:
    """Save geolocation data to the database only if it doesn't exist"""
    try:
        # First check if the lead already exists in geolocation table
        check_query = """
            SELECT id FROM geolocation WHERE id = %s AND type = 'lead'
        """
        
        with conn.cursor() as cur:
            cur.execute(check_query, (lead_id,))
            existing = cur.fetchone()
            
            if existing:
                print(f"  ⚠️  Lead ID {lead_id} already exists in geolocation table. Skipping insert.")
                return False
            
            # Insert new geolocation record
            insert_query = """
                INSERT INTO geolocation (id, latitude, longitude, type)
                VALUES (%s, %s, %s, 'lead')
            """
            
            cur.execute(insert_query, (
                lead_id,
                geo_data['latitude'],
                geo_data['longitude']
            ))
            conn.commit()
            print(f"  ✅ Inserted into geolocation table")
            return True
        
    except Exception as e:
        print(f"  ❌ Database error: {e}")
        conn.rollback()
        return False


def process_leads(limit: int = None):
    """Main function to fetch and save geolocation for leads
    """
    # Check if necessary config values are loaded
    if not all([PG_CONFIG['host'], GOOGLE_API_KEY]):
        print("❌ Configuration Error: Database host or API key not loaded. Check your .env file.")
        return

    conn = None
    results = []
    
    try:
        # Connect to database
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(**PG_CONFIG)
        print("✅ Connected successfully\n")
        
        # ... rest of the process_leads function logic ...
        
        # Fetch leads without geolocation
        print("📋 Fetching leads without geolocation...")
        all_leads = get_leads_without_geolocation(conn)
        print(f"✅ Found {len(all_leads)} total leads")
        
        # Apply limit if specified
        if limit:
            leads = all_leads[:limit]
            print(f"🎯 Processing first {len(leads)} leads (limit applied)\n")
        else:
            leads = all_leads
            print(f"🎯 Processing ALL {len(leads)} leads\n")
        
        print("=" * 80)
        
        if not leads:
            print("ℹ️  No leads to process. Exiting.")
            return
        
        # Process each lead
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for idx, lead in enumerate(leads, 1):
            lead_id = lead['id']
            address = build_address_string(lead)
            
            print(f"\n[{idx}/{len(leads)}] Lead ID: {lead_id}")
            print(f"{'─' * 80}")
            print(f"📍 Address: {address}")
            
            # Get geolocation
            geo_data = get_geolocation(address)
            
            if geo_data and 'latitude' in geo_data:
                print(f"✅ Geocoding SUCCESS")
                print(f"   Latitude:  {geo_data['latitude']}")
                print(f"   Longitude: {geo_data['longitude']}")
                print(f"   Formatted: {geo_data['formatted_address']}")
                
                # Save to database
                saved = save_geolocation(conn, lead_id, geo_data)
                
                if saved:
                    success_count += 1
                    results.append({
                        'lead_id': lead_id,
                        'status': 'success',
                        'latitude': geo_data['latitude'],
                        'longitude': geo_data['longitude'],
                        'formatted_address': geo_data['formatted_address']
                    })
                else:
                    skipped_count += 1
                    results.append({
                        'lead_id': lead_id,
                        'status': 'skipped',
                        'reason': 'Already exists in database'
                    })
            else:
                print(f"❌ Geocoding FAILED")
                print(f"   Error: {geo_data.get('error', 'Unknown')}")
                print(f"   Message: {geo_data.get('message', 'N/A')}")
                failed_count += 1
                
                results.append({
                    'lead_id': lead_id,
                    'status': 'failed',
                    'error': geo_data.get('error', 'Unknown'),
                    'message': geo_data.get('message', 'N/A')
                })
            
            # Rate limiting: Google API has limits, add delay between requests
            if idx < len(leads):
                time.sleep(0.2)  # 200ms delay between requests
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 SUMMARY")
        print("=" * 80)
        print(f"Total leads processed: {len(leads)}")
        print(f"✅ Successfully saved: {success_count}")
        print(f"⏭️  Skipped (already exists): {skipped_count}")
        print(f"❌ Failed: {failed_count}")
        print("=" * 80)
        
        return results
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")


if __name__ == "__main__":
    results = process_leads()