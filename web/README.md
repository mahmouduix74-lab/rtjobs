# وظايفي — personal web UI for RTJobs

موقع شخصي بيعرض الوظايف اللي السكرابر بيسحبها، من نفس قاعدة البيانات.

- **الوظايف:** بحث بالكلمات، استبعاد كلمات، فلترة بالموقع / المجال / التاريخ.
- **متابعة:** ★ حفظ · ✓ قدّمت · ✕ إخفاء (ضغطة تانية بتلغي) + ملاحظات لكل وظيفة.
- **تنبيه في الصفحة:** كل دقيقة بيشوف لو نزلت وظايف جديدة ويظهر شريط "في N وظيفة جديدة".
- **إحصائيات:** حالة كل موقع في آخر تشغيل، وظايف كل يوم، أكتر الشركات والمجالات.

بيقرا جداول السكرابر (`jobs`, `job_enrichments`, `runs`) **قراءة بس**.
الحفظ والملاحظات بتاعتك في ملف لوحده: `DATA_DIR/web.db`.

## التشغيل مع Docker (على السيرفر)

الـ service اسمها `web` في `docker-compose.override.yaml`، وDocker بيقرا الملف ده
تلقائياً مع `docker-compose.yaml` (مفصولين علشان تحديثات يوسف تتدمج من غير تعارض).

```bash
# في .env (اختياري):
WEB_PASSWORD=كلمة-سر-قوية     # من غيرها الموقع مفتوح لأي حد يوصله
WEB_PORT=8080

docker compose up -d --build web
```

الموقع بيسمع على `127.0.0.1:8080` بس (مش مفتوح للإنترنت). تفتحه بواحدة من دول:

1. **Tailscale (أسهل وأأمن):** نزّل Tailscale على السيرفر والموبايل، وعلى السيرفر:
   `sudo tailscale serve --bg 8080` → افتح `https://<اسم-السيرفر>.<tailnet>.ts.net` من موبايلك.
2. **SSH tunnel:** `ssh -L 8080:localhost:8080 user@server` ثم افتح `http://localhost:8080`.
3. **دومين عام:** reverse proxy زي Caddy قدامه مع HTTPS، ولازم `WEB_PASSWORD`.

## التشغيل محلياً من غير Docker

```bash
pip install -r web/requirements.txt
DATA_DIR=. uvicorn web.app:app --port 8000      # DATA_DIR = الفولدر اللي فيه rtjobs.db
```

### تجربة بداتا وهمية

```bash
DATA_DIR=/tmp/rtjobs-demo TELEGRAM_TOKEN=x TELEGRAM_CHAT_ID=x LOG_FILE= python -m web.demo_seed
DATA_DIR=/tmp/rtjobs-demo uvicorn web.app:app --port 8000
```

## خلّي السكرابر يجيب مجالك بس

في `.env`:

```dotenv
# LinkedIn: اعمل البحث على الموقع بالفلاتر اللي عايزها، رتّب بـ Most recent، وانسخ الرابط
LINKEDIN_SEARCH_URL=https://www.linkedin.com/jobs/search/?keywords=python%20developer&geoId=106155005&sortBy=DD
# Wuzzuf: نفس الفكرة (خلي start=0)
WUZZUF_SEARCH_URL=https://wuzzuf.net/search/jobs?q=python%20developer&start=0
```

وبعدين `docker compose up -d scraper`.

## الإعدادات

| متغير | الافتراضي | |
|---|---|---|
| `DATA_DIR` | `.` | فولدر `rtjobs.db` و `web.db` |
| `WEB_PASSWORD` | فاضي | لو موجود: الموقع يطلب كلمة سر (أي اسم مستخدم) |
| `WEB_PORT` | `8080` | البورت على السيرفر (compose) |
| `WEB_TAXONOMY` | `markup/enrichment/taxonomy.json` | أسماء المجالات |
| `TZ` | `Africa/Cairo` | التوقيت في العرض |

## تحديثات يوسف (y2youssef/rtjobs)

`.github/workflows/sync-upstream.yml` بيشتغل كل يوم الساعة 6:17 الصبح (أو يدوي من
تبويب **Actions → Sync upstream → Run workflow**):

- لو فيه commits جديدة ومفيش تعارض → بيدمجها في `main` لوحده.
- لو فيه تعارض مع تعديلاتنا → مش بيلمس حاجة، وبيفتح issue فيه أسماء الملفات وخطوات الحل.

على السيرفر بعد كل تحديث: `git pull` ثم `scripts/deploy.sh` (أو `docker compose up -d --build`).

تعديلاتنا على ملفات يوسف قليلة عمداً: سطرين في `config.py` (`LINKEDIN_SEARCH_URL`)،
والباقي كله ملفات جديدة (`web/`، `docker-compose.override.yaml`، الـ workflow).
