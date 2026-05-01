const API = "http://localhost:3001/api"

export const api = {
  play: () => fetch(`${API}/play`, { method: "POST" }),
  pause: () => fetch(`${API}/pause`, { method: "POST" }),
  speed: (value: number) =>
    fetch(`${API}/speed`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    }),
  seek: (time: number) =>
    fetch(`${API}/seek`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ time }),
    }),
}
