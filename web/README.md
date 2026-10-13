# وظايفي — موقع وظايف عام فوق RTJobs

موقع مفتوح لأي حد بيدوّر على شغل، بيعرض الوظايف اللي السكرابر بيلمّها.

- **الوظايف:** بحث بالكلمات، استبعاد كلمات، فلترة بالموقع / المجال / التاريخ.
- **لكل زائر:** ★ حفظ · ✓ قدّمت · ✕ إخفاء + ملاحظات — محفوظين في متصفحه هو بس
  (localStorage)، مفيش حسابات ولا بيانات زوار على السيرفر.
- **تنبيه في الصفحة:** كل دقيقة بيشوف لو نزلت وظايف جديدة.
- **إحصائيات:** آخر تحديث من كل موقع، وظايف كل يوم، أكتر الشركات والمجالات.

لكل وظيفة بيظهر: العنوان، الشركة، المكان، مقتطف قصير من الوصف، والمهارات، ولينك الإعلان
الأصلي. الوصف الكامل مش بيتنشر.

## إزاي الأجزاء بتتكلم مع بعض

```
السيرفر (VPS):  scraper ──► rtjobs.db ──► sync (كل دقيقة)
                                               │
                                               ▼
                                    Turso (قاعدة بيانات أونلاين)
                                               │
Vercel:                                 الموقع (api/index.py)
```

- `web/sync.py` بيقرا `rtjobs.db` **قراءة بس** وبيبعت الوظايف الجديدة والتصنيفات اللي
  اتأخرت وآخر حالة لكل موقع لجداول `public_jobs` / `public_runs` في Turso. بيمسح من
  Turso أي وظيفة أقدم من `PUBLIC_RETENTION_DAYS` (45 يوم).
- `web/app.py` بيقرا الجداول دي بس (`web/store.py`: Turso عن طريق HTTP، أو ملف SQLite محلي).
- Vercel بيخزّن كل صفحة دقيقة (`s-maxage=60`)، فالزيارات الكتير مش بتضغط على Turso.

## الإعدادات

**على السيرفر** (في `.env` جنب `docker-compose.yaml`):

```dotenv
TURSO_DATABASE_URL=libsql://<اسم-القاعدة>-<اسمك>.turso.io
TURSO_AUTH_TOKEN=<التوكن>
LINKEDIN_ENABLED=false          # LinkedIn ممنوع يتنشر منه؛ سيبه مقفول في النسخة العامة
```

وبعدين: `docker compose up -d --build sync`

**على Vercel** (Project → Settings → Environment Variables): نفس
`TURSO_DATABASE_URL` و `TURSO_AUTH_TOKEN`، وبعدين Redeploy. من غيرهم الموقع بيشتغل ديمو
بـ 160 وظيفة وهمية.

| متغير | الافتراضي | |
|---|---|---|
| `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN` | فاضي | قاعدة Turso (السيرفر + Vercel) |
| `PUBLIC_DB` | `DATA_DIR/public.db` | ملف SQLite محلي لو مفيش Turso |
| `PUBLIC_RETENTION_DAYS` | `45` | الوظايف الأقدم من كده بتتشال من الموقع |
| `SYNC_INTERVAL_SECONDS` | `60` | كل قد إيه `sync` يبعت الجديد |
| `WEB_PASSWORD` | فاضي | لو موجود: الموقع كله يطلب كلمة سر (ومفيش كاش) |
| `TZ` | `Africa/Cairo` | التوقيت في العرض |

## تجربة محلياً بداتا وهمية

```bash
pip install -r web/requirements.txt
DATA_DIR=/tmp/rtjobs-demo TELEGRAM_TOKEN=x TELEGRAM_CHAT_ID=x LOG_FILE= python -m web.demo_seed
DATA_DIR=/tmp/rtjobs-demo python -m web.sync --once
DATA_DIR=/tmp/rtjobs-demo uvicorn web.app:app --port 8000
```

## تحديثات يوسف (y2youssef/rtjobs)

`.github/workflows/sync-upstream.yml` بيشتغل كل يوم الساعة 6:17 الصبح (أو يدوي من
تبويب **Actions → Sync upstream → Run workflow**):

- لو فيه commits جديدة ومفيش تعارض → بيدمجها في `main` لوحده.
- لو فيه تعارض مع تعديلاتنا → مش بيلمس حاجة، وبيفتح issue فيه أسماء الملفات وخطوات الحل.

على السيرفر بعد كل تحديث: `git pull` ثم `docker compose up -d --build`.

تعديلاتنا على ملفات يوسف قليلة عمداً: سطرين في `config.py` (`LINKEDIN_SEARCH_URL`)،
والباقي كله ملفات جديدة (`web/`، `api/`، `vercel.json`، `docker-compose.override.yaml`، الـ workflow).
