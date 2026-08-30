"""A real-world example: current weather for any city as an MCP tool.

Uses the free Open-Meteo APIs (https://open-meteo.com/) — no API key needed,
and no dependencies beyond the standard library.

Run it:

    python examples/weather_server.py

Then connect any MCP client, e.g.:

    claude mcp add --transport sse weather http://127.0.0.1:8000/sse

and ask: "what's the weather in Mumbai?"
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request

from easy_mcp import MCPServer, ToolError

server = MCPServer(
    port=8000,
    name="weather-demo",
    instructions="Current weather lookups by city name, powered by Open-Meteo.",
)

_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_CURRENT_FIELDS = ",".join(
    [
        "temperature_2m",
        "apparent_temperature",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    ]
)

_WEATHER_CODES = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing rime fog",
    51: "light drizzle",
    53: "drizzle",
    55: "dense drizzle",
    61: "light rain",
    63: "rain",
    65: "heavy rain",
    71: "light snow",
    73: "snow",
    75: "heavy snow",
    80: "rain showers",
    81: "rain showers",
    82: "violent rain showers",
    95: "thunderstorm",
}


def _get_json(url: str, params: dict[str, str | int | float]) -> dict:
    with urllib.request.urlopen(f"{url}?{urllib.parse.urlencode(params)}", timeout=10) as response:
        return json.load(response)


@server.tool(tags=("weather",), category="data", timeout=15.0)
def get_weather(city: str) -> dict:
    """Get the current weather for a city.

    Args:
        city: City name, e.g. "Mumbai" or "San Francisco".
    """
    places = _get_json(_GEOCODE_URL, {"name": city, "count": 1}).get("results")
    if not places:
        raise ToolError(f"no city found matching {city!r}")
    place = places[0]
    current = _get_json(
        _FORECAST_URL,
        {
            "latitude": place["latitude"],
            "longitude": place["longitude"],
            "current": _CURRENT_FIELDS,
        },
    )["current"]
    return {
        "city": place["name"],
        "country": place.get("country", ""),
        "temperature_c": current["temperature_2m"],
        "feels_like_c": current["apparent_temperature"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_kmh": current["wind_speed_10m"],
        "condition": _WEATHER_CODES.get(current["weather_code"], "unknown"),
    }


if __name__ == "__main__":
    server.run()
