# vm_monitor.py

import requests
import json
import time
import subprocess
import platform
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration Import ---
try:
    # This line imports SLACK_WEBHOOK_URL, VMS_TO_MONITOR, and COMMON_PORTS
    # from the local config.py file.
    from config import SLACK_WEBHOOK_URL, VMS_TO_MONITOR, COMMON_PORTS
except ImportError:
    # If config.py is missing (e.g., on a fresh clone), exit with an error.
    print("FATAL ERROR: Could not find 'config.py'.")
    print("Please create 'config.py' and ensure it contains SLACK_WEBHOOK_URL and VMS_TO_MONITOR.")
    exit(1)
# --- End Configuration Import ---


# --- Function to Check Multiple Methods ---

def check_tcp_port(ip_address, port, timeout=3):
    """Check if a specific TCP port is open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((ip_address, port))
        sock.close()
        return result == 0
    except:
        return False

def check_any_port_open(ip_address, ports=COMMON_PORTS):
    """Check if ANY common port is open on the server."""
    for port in ports:
        if check_tcp_port(ip_address, port, timeout=2):
            return True, port
    return False, None

def check_http_service(ip_address):
    """Try to reach server via HTTP/HTTPS."""
    # Temporarily suppress InsecureRequestWarning from `verify=False`
    from urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    
    for protocol in ['http', 'https']:
        for port in [80, 443, 8080, 3000, 8000]:
            try:
                response = requests.get(
                    f"{protocol}://{ip_address}:{port}",
                    timeout=3,
                    verify=False,
                    allow_redirects=True
                )
                if response.status_code < 500:  # Any response except 5xx is good
                    return True, f"{protocol}:{port}"
            except:
                continue
    return False, None

def check_vm_status_ping(ip_address):
    """Check VM status using ping command."""
    if platform.system().lower() == "windows":
        # Windows ping command: -n packets, -w timeout in ms
        ping_command = ["ping", "-n", "2", "-w", "3000", ip_address]
    else:
        # Linux/macOS ping command: -c packets, -W timeout in seconds
        ping_command = ["ping", "-c", "2", "-W", "3", ip_address]
    
    try:
        result = subprocess.run(
            ping_command,
            capture_output=True,
            text=True,
            timeout=10,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except:
        return False

def comprehensive_server_check(ip_address):
    """
    Performs comprehensive check using multiple methods.
    Returns: (status, method_used)
    """
    
    # Method 1: Try common ports (fastest)
    port_open, port = check_any_port_open(ip_address)
    if port_open:
        return "UP", f"TCP Port {port}"
    
    # Method 2: Try HTTP/HTTPS
    http_works, service = check_http_service(ip_address)
    if http_works:
        return "UP", f"HTTP {service}"
    
    # Method 3: Try Ping (last resort)
    if check_vm_status_ping(ip_address):
        return "UP", "ICMP Ping"
    
    # All methods failed
    return "DOWN", "All checks failed"

# --- Function to Check Single VM ---

def check_single_vm(vm_name, ip_address):
    """Check status of a single VM using comprehensive checks."""
    print(f"⏳ Checking {vm_name} ({ip_address})...")
    
    status, method = comprehensive_server_check(ip_address)
    
    icon = "✅" if status == "UP" else "❌"
    print(f"   {icon} {vm_name}: {status} ({method})")
    
    return {
        "name": vm_name,
        "ip": ip_address,
        "status": status,
        "method": method,
        "timestamp": time.time()
    }

# --- Function to Send Slack Alert for Down Server ---

def send_slack_notification(vm_name, ip_address, status, method=None):
    """Sends a beautifully formatted alert to Slack when a server is down."""
    
    status_emoji = {
        "DOWN": "🔴",
        "TIMEOUT": "⏱️",
        "ERROR": "⚠️"
    }
    
    emoji = status_emoji.get(status, "❌")
    method_text = f"\n*Check Method:* All methods tried (TCP ports, HTTP, Ping)" if method == "All checks failed" else f"\n*Last Successful Check:* {method}"
    
    slack_data = {
        "text": f"{emoji} ALERT: {vm_name} is {status}!",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 SERVER DOWN ALERT",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Server Name:*\n`{vm_name}`"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*IP Address:*\n`{ip_address}`"
                    }
                ]
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{emoji} *{status}*"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Detected At:*\n<!date^{int(time.time())}^{{time}} {{date_short}}|{time.strftime('%Y-%m-%d %H:%M:%S')}>"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"⚡ *Action Required:* Server is not responding to any connectivity checks.{method_text}"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )

        if response.status_code == 200:
            print(f"   ✅ Slack notification sent for {vm_name}")
            return True
        else:
            print(f"   ❌ Slack notification failed. Status: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Error sending Slack notification: {e}")
        return False

# --- Function to Send Summary Report ---

def send_summary_notification(down_servers, total_servers):
    """Sends a summary notification if multiple servers are down."""
    
    if not down_servers:
        return
    
    down_count = len(down_servers)
    
    # Create fields for each down server
    server_fields = []
    for server in down_servers:
        server_fields.append({
            "type": "mrkdwn",
            "text": f"*{server['name']}*\n`{server['ip']}` - _{server['status']}_"
        })
    
    # Slack limits the number of fields in a section, so group them for large lists
    summary_section = {
        "type": "section",
        "fields": server_fields[:10]  # Show the first 10 in the main section
    }

    slack_data = {
        "text": f"⚠️ MONITORING SUMMARY: {down_count}/{total_servers} servers are DOWN",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 Monitoring Summary Report",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{down_count} out of {total_servers} servers are currently DOWN* 🔴"
                }
            },
            {
                "type": "divider"
            },
            summary_section,
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Scan completed at <!date^{int(time.time())}^{{time}} {{date_long}}|{time.ctime()}>"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"\n✅ Summary report sent to Slack")
            
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error sending summary: {e}")

# --- Main Execution Logic ---

if __name__ == "__main__":
    
    print("="*60)
    print("🔍 VM MONITORING SYSTEM - Starting Health Check")
    print("="*60)
    print(f"📊 Monitoring {len(VMS_TO_MONITOR)} servers...")
    print(f"⏰ Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 Using: TCP Port Scan + HTTP Check + Ping\n")
    
    results = []
    down_servers = []
    
    # Use ThreadPoolExecutor for parallel checking (faster)
    # Set max_workers to a reasonable number (e.g., 10 or 2 * num_cores)
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all VM checks
        future_to_vm = {
            executor.submit(check_single_vm, vm_name, ip_addr): vm_name 
            for vm_name, ip_addr in VMS_TO_MONITOR.items()
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_vm):
            vm_name = future_to_vm[future]
            try:
                result = future.result()
                results.append(result)
                
                # Send individual alert if server is down
                if result['status'] != "UP":
                    down_servers.append(result)
                    send_slack_notification(
                        result['name'], 
                        result['ip'], 
                        result['status'],
                        result.get('method')
                    )
                    
            except Exception as e:
                # Handle unexpected thread or check failure
                error_result = {
                    "name": vm_name,
                    "ip": VMS_TO_MONITOR[vm_name],
                    "status": "ERROR",
                    "method": str(e),
                    "timestamp": time.time()
                }
                results.append(error_result)
                down_servers.append(error_result)
                print(f"   ⚠️ Exception checking {vm_name}: {e}")
                send_slack_notification(vm_name, VMS_TO_MONITOR[vm_name], "ERROR", str(e))


    # --- Final Summary ---
    print("\n" + "="*60)
    print("📈 MONITORING SUMMARY")
    print("="*60)
    
    total_servers = len(results)
    up_count = total_servers - len(down_servers)
    
    print(f"✅ Servers UP: {up_count}/{total_servers}")
    print(f"❌ Servers DOWN/ERROR: {len(down_servers)}/{total_servers}")
    
    if down_servers:
        print("\n🔴 DOWN / ERROR SERVERS:")
        for server in down_servers:
            print(f"   - {server['name']} ({server['ip']}) - {server['status']}")
        
        # Send summary notification if multiple servers are down/errored
        if len(down_servers) >= 1: # Send summary even for one, just to confirm scan finished
            send_summary_notification(down_servers, total_servers)
    else:
        print("\n✅ All servers are operational!")
    
    print("\n" + "="*60)
    print(f"✅ Monitoring completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)