<h1 align="center">🧠 MastermindBot</h1>

<p align="center">
  <b>A multilingual, feature-rich Telegram quiz bot built with Python, Telethon, and python-telegram-bot.</b><br>
  <i>Developed by</i> <a href="https://github.com/Hamza-Hafeel">Hamza Hafeel</a> 💻
</p>

---

<h2>🚀 About</h2>

<p>
MastermindBot is an interactive <b>Telegram quiz bot</b> that brings fun, multilingual trivia games to life.  
It includes <b>leaderboards</b>, <b>achievements</b>, <b>streak tracking</b>, and <b>dynamic visuals</b> powered by PostgreSQL and Pillow (PIL).
</p>

---

<h2>✨ Features</h2>

<ul>
  <li>🌍 <b>Multilingual support</b> (English, Arabic, Spanish, French, etc.)</li>
  <li>🎶 <b>Taylor Swift</b> & <b>Lyrics-based</b> quiz modes</li>
  <li>🏆 <b>Achievements</b> and <b>streak systems</b></li>
  <li>🖼️ <b>Dynamic leaderboard generation</b> using Pillow</li>
  <li>🗄️ <b>PostgreSQL integration</b> for scalability</li>
  <li>⚙️ <b>Admin tools</b> and anti-spam protections</li>
  <li>⚡ Fully asynchronous and responsive performance</li>
</ul>

---

<h2>⚙️ Installation & Setup</h2>

<h3>1️⃣ Clone the Repository</h3>

bash
```git clone git@github.com:Hamza-Hafeel/MastermindBot.git
cd MastermindBot
<h3>2️⃣ Install Python Dependencies</h3> <p>Make sure you have <b>Python 3.9+</b> installed, then run:</p> ```

``` pip install -r requirements.txt ```
<h4>📦 If you don’t have requirements.txt, create one:</h4>
```
aiohttp
aiolimiter
apscheduler
asyncpg
Pillow
pytz
requests
telethon
python-telegram-bot
certifi ```
<h3>3️⃣ Configure Your Bot</h3> <p>Edit <code>Data/config.json</code> and enter your details:</p>
json
Copy code
{
  "BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
  "API_ID": "YOUR_TELEGRAM_API_ID",
  "API_HASH": "YOUR_TELEGRAM_API_HASH",
  "REPORT_GROUP_ID": "-1001234567890",
  "POSTGRES_DSN": "postgresql://user:password@localhost:5432/MastermindBot"
}
⚠️ Never share your config.json publicly — it contains private credentials.

<h3>4️⃣ Setup PostgreSQL Database</h3> <p>Create a new PostgreSQL database for the bot:</p>
sql
Copy code
CREATE DATABASE MastermindBot;
<p>Then update your connection string in <code>config.json</code>.</p>
<h3>5️⃣ Run the Bot 🎯</h3>
bash
Copy code
python Bot.py
<p>Now open Telegram and type <code>/start</code> to begin!</p>
<h2>💬 Bot Commands</h2> <table> <tr><th>Command</th><th>Description</th></tr> <tr><td><code>/start</code></td><td>Start or restart the bot</td></tr> <tr><td><code>/leaderboard</code></td><td>View group leaderboard</td></tr> <tr><td><code>/streak</code></td><td>Check streak rankings</td></tr> <tr><td><code>/profile</code></td><td>View your stats and achievements</td></tr> <tr><td><code>/settings</code></td><td>Access admin settings panel</td></tr> <tr><td><code>/reportquestion</code></td><td>Report a wrong question</td></tr> <tr><td><code>/stats</code></td><td>View global statistics</td></tr> <tr><td><code>/sq</code></td><td>Send manual question (admin only)</td></tr> </table>
<h2>🏆 Achievements</h2> <table> <tr><th>Achievement</th><th>Requirement</th></tr> <tr><td>🥉 Fearless Beginner</td><td>Answer 5 questions</td></tr> <tr><td>🥈 Love Story Enthusiast</td><td>Answer 50 questions</td></tr> <tr><td>🥇 Reputation Legend</td><td>Answer 1000 questions</td></tr> <tr><td>🔥 Speak Now Streak</td><td>Maintain a 30+ day streak</td></tr> <tr><td>💫 Swiftie Supreme</td><td>Unlock all achievements</td></tr> </table>
<h2>🖼️ Dynamic Leaderboards</h2> <p> Leaderboards and streak banners are generated dynamically using <b>Pillow (PIL)</b> with multilingual fonts and templates. </p>
<h2>🔒 Security</h2> <ul> <li>⚠️ <code>config.json</code> and <code>.session</code> files are excluded via <b>.gitignore</b></li> <li>🔑 Admin-only commands are protected</li> <li>🧩 Channel join enforcement enabled</li> <li>🚫 Spam prevention & flood control built-in</li> </ul>
<h2>💾 Developer Commands</h2>
bash
Copy code
# Pull latest updates
git pull

# Add and push new changes
git add .
git commit -m "Updated features"
git push
<h2>💖 Support</h2> <p>If you’d like to support ongoing development:</p> <ul> <li>💰 <b>PayPal:</b> <a href="https://paypal.me/NexusModWorks">paypal.me/NexusModWorks</a></li> <li>⭐ <b>Telegram Stars:</b> Available in the bot’s donation menu</li> </ul>
<h2>👨‍💻 Author</h2> <p> <b>Hamza Hafeel</b><br> 🎓 Information Science & Engineering Student<br> 💬 Telegram: <a href="https://t.me/HamzaHafeel">@HamzaHafeel</a><br> 🌐 GitHub: <a href="https://github.com/Hamza-Hafeel">Hamza-Hafeel</a> </p>
<h2>📜 License</h2> <p> This project is licensed under the <b>MIT License</b>. You may modify, reuse, and distribute it — but please give proper credit. </p>
<h3 align="center">💻 Built with ❤️, Python, and dedication ☕</h3>
