import asyncio
from bleak import BleakScanner, BleakClient

POD_ADDRESS = "F2:9A:41:2B:5B:55"

NUS_TX_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # notify

def on_notify(_, data: bytearray):
    print(data.decode("utf-8", errors="ignore"), end="")

async def main():
    print("Finding device by address...")
    dev = await BleakScanner.find_device_by_address(POD_ADDRESS, timeout=10.0)
    if not dev:
        raise SystemExit("Not found. Make sure it's powered and NOT connected to BLE Explorer.")

    print("Connecting...")
    async with BleakClient(dev) as client:
        print("Connected. Subscribing...")
        await client.start_notify(NUS_TX_UUID, on_notify)
        print("Receiving... (Ctrl+C to stop)")
        while True:
            await asyncio.sleep(1)

asyncio.run(main())