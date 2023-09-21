import httpx  # requests capability, but can work with async
from prefect import flow, task, get_run_logger, serve
from prefect.artifacts import create_link_artifact, create_table_artifact
from prefect.tasks import task_input_hash
from prefect.events import emit_event


@task(cache_key_fn=task_input_hash)
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp

@task
def save_table():
    highest_churn_possibility = [
        {'customer_id': '12345', 'name': 'John Smith', 'churn_probability': 0.85},
        {'customer_id': '56789', 'name': 'Jane Jones', 'churn_probability': 0.65}
    ]

    create_table_artifact(
        key="personalized-reachout",
        table=highest_churn_possibility,
        description="# Marvin, please reach out to these customers today!"
    )

@task(retries=4)
def save_weather(temp: float):
    logger = get_run_logger()
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    logger.warning("Successfully wrote temp")
    emit_event(
        event="event.org.weatherman.save_weather",
        resource={
            "prefect.resource.id": "event.org.weatherman",
        },
        payload={
            "data": str(temp)
        }
    )
    return "Successfully wrote temp"

@flow
def pipeline(lat: float = 12.1, lon: float = 32.0):
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    pipeline_two(lat + 1, lon + 2)
    return result

@flow
def pipeline_two(lat: float = 12.1, lon: float = 32.0):
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    return result


if __name__ == "__main__":
    pipeline()
