# 💡 Lumos — Power Outage Tracker Telegram Bot

A Telegram bot that monitors planned power outage schedules from Ukrainian energy providers and sends real-time notifications to subscribers.

## Features

- **Queue-based subscriptions** — subscribe to one or multiple power queues (1.1–6.2)
- **Address lookup** — find your queue automatically by entering your address
- **Real-time notifications** — get alerted when schedules change or new ones appear
- **Smart reminders** — configurable reminders before power on/off events (5 min – 2 hours)
- **Multi-date schedules** — view outage schedules for today and upcoming days
- **Duration display** — shows total outage hours per day and per time slot
- **Donation support** — built-in Monobank jar integration

## Tech Stack

| Component | Technology |
|-----------|------------|
| Bot framework | [aiogram 3](https://docs.aiogram.dev/) |
| Database | [MongoDB](https://www.mongodb.com/) via [Motor](https://motor.readthedocs.io/) |
| HTTP client | [curl_cffi](https://github.com/yifeikong/curl_cffi) (Chrome TLS fingerprint) |
| Web server | [aiohttp](https://docs.aiohttp.org/) |
| Proxy support | SOCKS5/HTTP via `PROXY_URL` env variable |
| Hosting | [Render](https://render.com/) (or any VPS) |

## Project Structure

```
├── main.py              # Bot logic, handlers, scheduler, web server
├── requirements.txt     # Python dependencies
├── render.yaml          # Render deployment config
├── templates/
│   └── index.html       # Landing page (status & info)
├── .env                 # Environment variables (not in repo)
└── .gitignore
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | ✅ | Telegram Bot API token |
| `MONGO_URI` | ✅ | MongoDB connection string |
| `DB_NAME` | ❌ | Database name (default: `lumos_bot`) |
| `CHECK_INTERVAL` | ❌ | Schedule check interval in seconds (default: `45`) |
| `PORT` | ❌ | Web server port (default: `8080`) |
| `APQE_PQFRTY` | ✅ | API endpoint for queue schedule |
| `APSRC_PFRTY` | ✅ | API endpoint for address search |
| `PROXY_URL` | ❌ | Proxy URL (e.g. `socks5://user:pass@ip:port`) |

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-username/svito_bot.git
cd svito_bot
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment

Create a `.env` file in the project root:

```env
BOT_TOKEN=your_telegram_bot_token
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
APQE_PQFRTY=https://...
APSRC_PFRTY=https://...
PROXY_URL=socks5://user:pass@ip:port
```

### 5. Run

```bash
python main.py
```

## Deploy to Render

1. Push code to a GitHub repository
2. Create a new **Web Service** on [Render](https://render.com/)
3. Connect your repository
4. Render will auto-detect `render.yaml` and configure the service
5. Add environment variables in the Render dashboard

## How It Works

1. **Scheduler** polls the energy provider API every `CHECK_INTERVAL` seconds
2. Compares current schedule with the stored state in MongoDB
3. If changes are detected, sends notifications to all subscribers of affected queues
4. **Reminder checker** runs every minute and sends configurable alerts before outage events

## Bot Commands

| Command/Button | Description |
|----------------|-------------|
| `/start` | Start the bot, show main menu |
| `/help` | Show usage instructions |
| 🔄 Check schedule | View current schedules for your queues |
| 📋 My subscriptions | View subscription status |
| ⚡ Select queues | Subscribe by address or queue number |
| ✏️ Manage queues | Add/remove queues, toggle reminders |
| 💛 Support project | Donation link |

## License

This project is provided as-is for personal and educational use.
