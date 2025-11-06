<h1 align="center">🧠 MastermindBot</h1>

<p align="center">
  <b>A multilingual, feature-rich Telegram quiz bot built with Python, Telethon, and python-telegram-bot.</b><br>
  <i>Developed and maintained by</i> <a href="https://github.com/Hamza-Hafeel">Hamza Hafeel</a> 💻
</p>

---

<h2>🚀 About</h2>

<p>
MastermindBot is an interactive <b>Telegram quiz bot</b> that delivers fun and educational trivia experiences in multiple languages.  
It includes <b>leaderboards</b>, <b>achievements</b>, <b>streaks</b>, and <b>dynamic images</b> powered by PostgreSQL and Pillow (PIL).  
Designed for both <b>private chats</b> and <b>Telegram groups</b>.
</p>

---

<h2>✨ Features</h2>

<ul>
  <li>🌍 <b>Multilingual support</b> — English, Arabic, French, German, Italian, Spanish, Russian, Korean, Japanese, Portuguese, Turkish, and more</li>
  <li>🎶 <b>Taylor Swift</b> & <b>Lyrics-based</b> quiz modes</li>
  <li>🏆 <b>Achievements</b> and <b>streak rewards</b> for dedicated players</li>
  <li>🖼️ <b>Dynamic leaderboard generation</b> using Pillow (PIL)</li>
  <li>🗄️ <b>PostgreSQL integration</b> for scalable data storage</li>
  <li>⚙️ <b>Admin tools</b>, group management, and moderation commands</li>
  <li>🔒 <b>Join-channel enforcement</b> and anti-spam systems</li>
  <li>⚡ Fully asynchronous for smooth, lag-free performance</li>
</ul>

---

<h2>⚙️ Installation & Setup</h2>

<h3>1️⃣ Clone the Repository</h3>

bash
git clone git@github.com:Hamza-Hafeel/MastermindBot.git
cd MastermindBot
<h3>2️⃣ Install Python Dependencies</h3> <p>Ensure you have <b>Python 3.9+</b> installed, then install all dependencies:</p>
bash
Copy code
pip install -r requirements.txt
<h4>📦 requirements.txt</h4>
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
<h3>3️⃣ Configure Your Bot</h3> <p>Open <code>Data/config.json</code> and fill in your bot credentials:</p>
json
Copy code
{
  "BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
  "API_ID": "YOUR_TELEGRAM_API_ID",
  "API_HASH": "YOUR_TELEGRAM_API_HASH",
  "REPORT_GROUP_ID": "-1001234567890",
  "POSTGRES_DSN": "postgresql://user:password@localhost:5432/MastermindBot"
}
<p><b>⚠️ Important:</b> Never upload your <code>config.json</code> publicly — it contains sensitive data.</p>
<h3>4️⃣ Setup PostgreSQL Database</h3> <p>Create your bot's database using PostgreSQL:</p>
sql
Copy code
CREATE DATABASE MastermindBot;
<p>Update the connection string in <code>config.json</code> accordingly.</p>
<h3>5️⃣ Run the Bot 🎯</h3>
bash
Copy code
python Bot.py
<p>Once running, open Telegram and start your bot using <code>/start</code>.</p>
<h2>💬 Commands</h2> <table> <tr><th>Command</th><th>Description</th></tr> <tr><td><code>/start</code></td><td>Start or restart the bot</td></tr> <tr><td><code>/leaderboard</code></td><td>View current leaderboard</td></tr> <tr><td><code>/streak</code></td><td>View current streak rankings</td></tr> <tr><td><code>/profile</code></td><td>Show your stats and achievements</td></tr> <tr><td><code>/settings</code></td><td>Admin panel for configuration</td></tr> <tr><td><code>/reportquestion</code></td><td>Report incorrect questions</td></tr> <tr><td><code>/stats</code></td><td>Global bot statistics</td></tr> <tr><td><code>/sq</code></td><td>Send manual question (admin only)</td></tr> </table>
<h2>🏆 Achievements</h2> <table> <tr><th>Achievement</th><th>Requirement</th></tr> <tr><td>🥉 <b>Fearless Beginner</b></td><td>Answer 5 questions</td></tr> <tr><td>🥈 <b>Love Story Enthusiast</b></td><td>Answer 50 questions</td></tr> <tr><td>🥇 <b>Reputation Legend</b></td><td>Answer 1000 questions</td></tr> <tr><td>🔥 <b>Speak Now Streak</b></td><td>Maintain a 30+ day streak</td></tr> <tr><td>💫 <b>Swiftie Supreme</b></td><td>Unlock all achievements</td></tr> </table>
<h2>🖼️ Dynamic Leaderboards</h2> <p> All leaderboard and streak cards are dynamically generated using <b>Pillow (PIL)</b>. Fonts and templates adapt automatically to different languages for global support. </p>
<h2>🔒 Security</h2> <ul> <li>Sensitive files (<code>config.json</code>, <code>.session</code>) are excluded via <b>.gitignore</b></li> <li>Admin-only commands require elevated privileges</li> <li>Automatic channel join enforcement and spam protection</li> <li>Error handling and anti-flood systems built-in</li> </ul>
<h2>💾 Developer Commands</h2>
bash
Copy code
# Pull latest updates
git pull

# Add and push your new changes
git add .
git commit -m "Updated bot features"
git push
<h2>💖 Support</h2> <p> If you love this bot and want to support its development 💜 Consider donating to help with hosting and updates: </p> <ul> <li>💰 <b>PayPal:</b> <a href="https://paypal.me/NexusModWorks">paypal.me/NexusModWorks</a></li> <li>⭐ <b>Telegram Stars:</b> Available within the bot’s donation menu</li> </ul>
<h2>👨‍💻 Author</h2> <p> <b>Hamza Hafeel</b><br> 🎓 Information Science & Engineering Student<br> 💬 Telegram: <a href="https://t.me/HamzaHafeel">@HamzaHafeel</a><br> 🌐 GitHub: <a href="https://github.com/Hamza-Hafeel">Hamza-Hafeel</a> </p>
<h2>📜 License</h2> <p> This project is licensed under the <b>MIT License</b>.<br> You are free to use, modify, and distribute it responsibly — just credit the original author. </p>
<h3 align="center">💻 Built with ❤️, Python, and caffeine ☕</h3>
