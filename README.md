MastermindBot
A multilingual, feature-rich Telegram quiz bot built with Python, Telethon, and python-telegram-bot
MastermindBo.t is an interactive Telegram quiz bot that brings engaging, multilingual trivia games to Telegram groups.
It includes leaderboards, achievements, streak tracking, and image-based scoreboards, all powered by a PostgreSQL backend and dynamic image generation.

🚀 Features
✅ Multilingual quizzes (English, Arabic, French, German, Italian, Spanish, Russian, Korean, Japanese, Portuguese, Turkish, and more)
✅ Taylor Swift & Lyrics-based quiz modes 🎶
✅ Achievements and streak systems to reward consistent players
✅ Dynamic leaderboards using custom image templates
✅ Secure PostgreSQL integration for scalable data management
✅ Real-time score updates with caching and async operations
✅ Join-channel enforcement, banlist, and admin commands
✅ Responsive design — works perfectly in both private chats and groups

🗂️ Folder Structure
MastermindBot/
├── Bot.py                      # Main bot script (entry point)
│
├── Data/
│   ├── config.json             # Contains API keys, database DSN, etc.
│   ├── achievements.json       # Achievement thresholds and names
│   ├── banned_groups.json      # List of restricted groups
│   ├── excepted_groups.json    # Groups excluded from limitations
│   ├── localization.json       # Translations for supported languages
│   ├── logo.png                # Bot logo or branding
│
├── Fonts/                      # Fonts for image-based leaderboards
│   ├── NotoSans-ExtraBold.ttf
│   ├── NotoSansJP-ExtraBold.ttf
│   ├── NotoSansKR-ExtraBold.ttf
│   ├── NotoNaskhArabic-Bold.ttf
│
├── Leaderboard-Template/       # Base images for multilingual leaderboards
│   ├── Leaderboard_en.jpg
│   ├── Leaderboard_es.jpg
│   ├── Leaderboard_ar.jpg
│   ├── Leaderboard_fr.jpg
│   └── ... (other languages)
│
├── Streaks-Template/           # Base images for streak visuals
│   ├── Streaks_en.jpg
│   ├── Streaks_es.jpg
│   ├── Streaks_fr.jpg
│   └── ... (other languages)
│
├── Questions/                  # Quiz question data
│   ├── General_questions.json
│   └── Lyrics_questions.json
│
├── requirements.txt            # Python dependencies
└── bot_session.session         # Telethon session (auto-generated)

⚙️ Installation & Setup
1️⃣ Clone the repository
git clone git@github.com:Hamza-Hafeel/MastermindBot.git
cd MastermindBot

2️⃣ Install Python dependencies
Make sure you have Python 3.9+ installed, then run:

pip install -r requirements.txt
requirements.txt
aiohttp
aiolimiter
apscheduler
asyncpg
Pillow
pytz
requests
telethon
python-telegram-bot
certifi

3️⃣ Configure your bot
Edit the file Data/config.json and replace it with your credentials:

{
  "BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
  "API_ID": "YOUR_TELEGRAM_API_ID",
  "API_HASH": "YOUR_TELEGRAM_API_HASH",
  "REPORT_GROUP_ID": "-1001234567890",
  "POSTGRES_DSN": "postgresql://user:password@localhost:5432/MastermindBot"
}
⚠️ Important: Never share your config.json publicly — it contains sensitive keys.

4️⃣ Setup PostgreSQL Database
Make sure PostgreSQL is running and create a database:
CREATE DATABASE MastermindBot;
Then update the connection string inside config.json accordingly.

5️⃣ Run the bot 🎯
Start your bot with:
python Bot.py
Once running, open Telegram and start your bot using /start.

💬 Bot Commands
Command	Description
/start	Start or restart the bot
/leaderboard	View group leaderboard
/streak	Show current streak rankings
/profile	View your stats and achievements
/settings	Admin panel to configure group settings
/reportquestion	Report incorrect questions
/stats	Show global bot statistics
/sq	Send question manually (admin only)

🏆 Achievements
Achievements are automatically unlocked as users play.
Examples:

Achievement	Requirement
🥉 Fearless Beginner	Answer 5 questions
🥈 Love Story Enthusiast	Answer 50 questions
🥇 Reputation Legend	Answer 1000 questions
🔥 Speak Now Streak	Maintain 30+ daily streaks
💫 Swiftie Supreme	Unlock all achievements
🖼️ Dynamic Leaderboards
The bot automatically generates image-based leaderboards in different languages using Pillow (PIL) and custom font files located in /Fonts/.
Each leaderboard image is based on the templates stored in /Leaderboard-Template/.

🔒 Security & Permissions
The bot enforces join requirements for certain channels.
Admin-only features are protected with role checks.
Sensitive data (tokens, DSNs) should never be shared publicly.
Banned and excepted group IDs are managed via JSON files.

💾 Useful Commands for Developers
# Pull latest updates from GitHub
git pull

# Add and push new local changes
git add .
git commit -m "Updated bot features"
git push

Hamza Hafeel
🎓 Information Science & Engineering Student
💬 Telegram: @HamzaHafeel
🌐 GitHub: Hamza-Hafeel

📜 License
This project is licensed under the MIT License —
You are free to use, modify, and distribute this code as long as proper credit is given.

🧩 Notes
Works perfectly on Ubuntu, Windows, and cloud VPS (Oracle, Render, etc.)
Recommended Python version: 3.10 or newer
Make sure all .json data files are UTF-8 encoded for multilingual compatibility.
