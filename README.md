<h1 align="center">🧠 MastermindBot</h1>
<p align="center">
  <b>A multilingual, feature-rich Telegram quiz bot built with Python, Telethon, and python-telegram-bot.</b><br>
  Developed by <a href="https://github.com/Hamza-Hafeel">Hamza Hafeel</a> 💻
</p>

---

## 🚀 About
MastermindBot is an interactive **Telegram quiz bot** that delivers fun, multilingual trivia experiences for groups and individuals.  
It includes **achievements, leaderboards, streaks, and personalized profiles**, all powered by a PostgreSQL backend and **dynamic image generation** with Pillow (PIL).

---

## ✨ Features
- 🌍 **Multilingual support** (English, Arabic, French, German, Italian, Spanish, Russian, Korean, Japanese, Portuguese, Turkish, and more)
- 🎶 **Taylor Swift & Lyrics-based** quiz modes
- 🏆 **Achievements and Streak Rewards** for active players
- 🖼️ **Dynamic image leaderboards** powered by Pillow
- 🗄️ **PostgreSQL integration** for scalable performance
- ⚙️ **Admin panel**, **banned/allowed group system**
- 🔒 **Join-channel enforcement** for access control
- ⚡ Fully asynchronous — smooth and responsive performance
- 💬 Works flawlessly in **private chats and group chats**

---

## ⚙️ Installation & Setup

### 1️⃣ Clone the Repository
```bash
git clone git@github.com:Hamza-Hafeel/MastermindBot.git
cd MastermindBot
2️⃣ Install Python Dependencies
Make sure you have Python 3.9+, then run:

bash
Copy code
pip install -r requirements.txt
requirements.txt
text
Copy code
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
3️⃣ Configure Your Bot
Edit the file Data/config.json and add your credentials:

json
Copy code
{
  "BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
  "API_ID": "YOUR_TELEGRAM_API_ID",
  "API_HASH": "YOUR_TELEGRAM_API_HASH",
  "REPORT_GROUP_ID": "-1001234567890",
  "POSTGRES_DSN": "postgresql://user:password@localhost:5432/MastermindBot"
}
⚠️ Never share your config.json publicly — it contains secrets and tokens.

4️⃣ Setup PostgreSQL Database
Make sure PostgreSQL is installed and create a database:

sql
Copy code
CREATE DATABASE MastermindBot;
Then update your config.json connection string with your database credentials.

5️⃣ Run the Bot 🎯
Once everything is configured:

bash
Copy code
python Bot.py
Then open Telegram and start your bot with /start.

💬 Bot Commands
Command	Description
/start	Start or restart the bot
/leaderboard	View group leaderboard
/streak	Show streak rankings
/profile	View personal achievements and stats
/settings	Admin settings menu
/reportquestion	Report incorrect questions
/stats	View global bot statistics
/sq	Send quiz question manually (admin only)

🏆 Achievements
Achievement	Requirement
🥉 Fearless Beginner	Answer 5 questions
🥈 Love Story Enthusiast	Answer 50 questions
🥇 Reputation Legend	Answer 1000 questions
🔥 Speak Now Streak	Maintain a 30+ day streak
💫 Swiftie Supreme	Unlock all achievements

🖼️ Dynamic Leaderboards
Leaderboards and streak banners are generated dynamically using Pillow (PIL).
Each image adapts to the user’s language and data, producing a clean and engaging visual for groups.

🔒 Security
Sensitive files like config.json and .session are excluded via .gitignore

Admin-only commands require elevated permissions

Channel join verification for user access

Safe exception handling for errors and spam control

💾 Developer Commands
bash
Copy code
# Pull the latest updates
git pull

# Add and push new commits
git add .
git commit -m "Updated bot features"
git push
💖 Support
If you enjoy using this bot and want to support further development:

💰 PayPal: paypal.me/NexusModWorks

⭐ Telegram Stars: Available within the bot’s donation system

👨‍💻 Author
Hamza Hafeel
🎓 Information Science & Engineering Student
💬 Telegram: @HamzaHafeel
🌐 GitHub: Hamza-Hafeel

📜 License
This project is licensed under the MIT License.
You are free to use, modify, and distribute it responsibly — with credit to the original author.

<h3 align="center">💻 Built with ❤️, Python, and endless dedication ☕</h3> ```
