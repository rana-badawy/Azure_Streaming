import asyncio
import json
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from data_generation import get_data


async def run():
    while True:
        await asyncio.sleep(10)

        producer = EventHubProducerClient.from_connection_string(
            conn_str="",
            eventhub_name="")

        async with producer:
            event_data_batch = await producer.create_batch()

            event_data_batch.add(EventData(json.dumps(get_data())))

            await producer.send_batch(event_data_batch)

            print("Data sent successfully")


loop = asyncio.get_event_loop()

try:
    asyncio.ensure_future(run())

    loop.run_forever()
except KeyboardInterrupt:
    print("Failed")
finally:
    print("Closing loop")

    loop.close()
