import os
import re
import json
import base64
import logging
import asyncio
import aiohttp
from urllib.parse import urlparse
from pyrogram import Client, filters
from pyrogram.types import Message
import zipfile

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = "961780"        # Dapatkan di my.telegram.org
API_HASH = "bbbfa43f067e1e8e2fb41f334d32a6a7"    # Dapatkan di my.telegram.org
BOT_TOKEN = "7818903497:AAHKM3JaPTScXL1qy80LDzUTrkZwLwUwBis" 
app = Client("tpmusic_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
# ===================================================

class AudioCache:
    def __init__(self, ttl_seconds=12*3600):
        self.ttl = ttl_seconds
        # Mapping dari tidal_id -> {"filepath": str, "title": str, "performer": str, "task": asyncio.Task}
        self.cache = {}

    async def _delete_after_ttl(self, tidal_id, filepath):
        try:
            await asyncio.sleep(self.ttl)
            if tidal_id in self.cache:
                del self.cache[tidal_id]
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    logger.info(f"File dihapus dari disk setelah {self.ttl/3600} jam tidak diakses: {filepath}")
                except Exception as e:
                    logger.error(f"Gagal menghapus file {filepath}: {e}")
        except asyncio.CancelledError:
            # Task dibatalkan karena ada request baru (timer direset)
            pass

    def get(self, tidal_id):
        if tidal_id in self.cache:
            entry = self.cache[tidal_id]
            
            # Verifikasi file masih ada di disk
            if not os.path.exists(entry["filepath"]):
                del self.cache[tidal_id]
                return None
                
            # Cancel task lama
            entry["task"].cancel()
            
            # Buat task baru (perpanjang 12 jam)
            entry["task"] = asyncio.create_task(self._delete_after_ttl(tidal_id, entry["filepath"]))
            logger.info(f"Cache diperpanjang untuk ID {tidal_id}")
            return entry
        return None

    def put(self, tidal_id, filepath, title, performer):
        # Jika sudah ada di cache, batalkan task penghapusan yang lama
        if tidal_id in self.cache:
            self.cache[tidal_id]["task"].cancel()
        
        # Buat task penghapusan baru (setelah 12 jam)
        task = asyncio.create_task(self._delete_after_ttl(tidal_id, filepath))
        
        # Simpan ke cache
        self.cache[tidal_id] = {
            "filepath": filepath,
            "title": title,
            "performer": performer,
            "task": task
        }
        logger.info(f"Ditambahkan ke cache untuk ID {tidal_id} pada path {filepath}")

    def clear_leftovers(self, output_dir="downloads"):
        """Menghapus file sisa dari run sebelumnya untuk mencegah storage leak."""
        if os.path.exists(output_dir):
            for filename in os.listdir(output_dir):
                filepath = os.path.join(output_dir, filename)
                try:
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                        logger.info(f"Membersihkan file leftover saat startup: {filepath}")
                except Exception as e:
                    logger.error(f"Gagal membersihkan file leftover {filepath}: {e}")

# Inisialisasi Cache Global
audio_cache = AudioCache()

class AsyncUniversalDownloader:
    def __init__(self, output_dir="downloads"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.mirrors = [
            "https://vogel.qqdl.site", "https://wolf.qqdl.site",
            "https://hund.qqdl.site", "https://katze.qqdl.site",
            "https://maus.qqdl.site", "https://tidal-api.binimum.org",
            "https://tidal.kinoplus.online", "https://hifi-one.spotisaver.net",
            "https://hifi-two.spotisaver.net", "https://triton.squid.wtf"
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    def clean_filename(self, text):
        return re.sub(r'[\\/*?:"<>|]', "", text)

    async def fetch_spotify_playlist_tracks(self, session, url):
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    matches = re.findall(r'<meta name="music:song" content="(.*?)"', html)
                    title_match = re.search(r'<title>(.*?)</title>', html)
                    title = title_match.group(1).split('|')[0].strip() if title_match else "Spotify_Playlist"
                    return title, matches
        except Exception as e:
            logger.error(f"Gagal fetch playlist: {e}")
        return None, []

    async def fetch_spotify_metadata(self, session, url):
        if "spotify.com" not in url:
            return None
        api_url = f"https://open.spotify.com/oembed?url={url}"
        try:
            async with session.get(api_url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    title = data.get("title", "Unknown")
                    artist = data.get("author_name", "Unknown")
                    return {
                        "title": title,
                        "artist": artist,
                        "album": "Unknown"
                    }
        except Exception as e:
            logger.error(f"Gagal fetch spotify metadata: {e}")
        return None

    async def resolve_with_songlink(self, session, input_url):
        api_url = f"https://api.song.link/v1-alpha.1/links?url={input_url}"
        tidal_id, deezer_id = None, None
        try:
            async with session.get(api_url, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    entities = data.get("entitiesByUniqueId", {})
                    for key, entity in entities.items():
                        if entity.get("apiProvider") == "tidal" and entity.get("type") == "song":
                            tidal_id = entity.get("id")
                        elif entity.get("apiProvider") == "deezer" and entity.get("type") == "song":
                            deezer_id = entity.get("id")
                    return tidal_id, deezer_id
        except Exception as e:
            logger.error(f"Gagal resolve song.link API: {e}")
            
        # Fallback to scraping the public page if API fails or returns non-200
        logger.info(f"Fallback to song.link web scrape for: {input_url}")
        page_url = f"https://song.link/{input_url}"
        try:
            async with session.get(page_url, timeout=15) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
                    if match:
                        data = json.loads(match.group(1))
                        page_props = data.get('props', {}).get('pageProps', {})
                        page_data = page_props.get('pageData', {})
                        sections = page_data.get('sections', [])
                        
                        for section in sections:
                            for link in section.get('links', []):
                                platform = link.get('platform', '').lower()
                                unique_id = link.get('uniqueId', '')
                                if platform == 'tidal' and 'song' in unique_id:
                                    tidal_id = re.split(r'[|:]+', unique_id)[-1]
                                elif platform == 'deezer' and 'song' in unique_id:
                                    deezer_id = re.split(r'[|:]+', unique_id)[-1]
                        return tidal_id, deezer_id
        except Exception as e:
            logger.error(f"Gagal fallback scrape song.link: {e}")
            
        return tidal_id, deezer_id

    async def fetch_metadata(self, session, deezer_id):
        if not deezer_id: return None
        api_url = f"https://api.deezer.com/track/{deezer_id}"
        try:
            async with session.get(api_url, timeout=10) as resp:
                data = await resp.json()
                if "error" not in data:
                    return {
                        "title": data.get("title", "Unknown"),
                        "artist": data.get("artist", {}).get("name", "Unknown"),
                        "album": data.get("album", {}).get("title", "Unknown"),
                        "isrc": data.get("isrc", "Unknown"),
                        "cover": data.get("album", {}).get("cover_xl", None)
                    }
        except Exception as e:
            logger.error(f"Gagal fetch metadata: {e}")
        return None

    async def query_mirror(self, session, mirror_base, tidal_id, quality="LOSSLESS"):
        url = f"{mirror_base}/track/?id={tidal_id}&quality={quality}"
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "data" in data and "manifest" in data["data"]:
                        return data
        except:
            pass
        return None

    async def fetch_manifest_parallel(self, session, tidal_id, quality="LOSSLESS"):
        # FAST FAN-OUT: Hit semua mirror bersamaan
        tasks = [asyncio.create_task(self.query_mirror(session, m, tidal_id, quality)) for m in self.mirrors]
        
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                result = task.result()
                if result:
                    # Cancel sisa request
                    for t in tasks: t.cancel()
                    return result
        return None

    def decode_manifest(self, data_json):
        try:
            manifest_b64 = data_json.get("data", {}).get("manifest")
            if not manifest_b64: return None
            manifest_json_str = base64.b64decode(manifest_b64).decode('utf-8')
            manifest_data = json.loads(manifest_json_str)
            urls = manifest_data.get("urls", [])
            return urls[0] if urls else None
        except:
            return None

    async def download_file(self, session, url, metadata, tidal_id):
        ext = urlparse(url).path.split('.')[-1] if '.' in urlparse(url).path else 'flac'
        if metadata:
            filename = f"{self.clean_filename(metadata['artist'])} - {self.clean_filename(metadata['title'])}.{ext}"
        else:
            filename = f"track_{tidal_id}.{ext}"
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            async with session.get(url) as resp:
                with open(filepath, 'wb') as f:
                    # Chunk besar 1MB untuk speed I/O stream, tanpa progress bar
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
                                
            return filepath, metadata
        except Exception as e:
            logger.error(f"Download gagal: {e}")
            return None, None

# ================= HANDLER BOT =================

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    await message.reply_text(
        "👋 Halo! Kirimkan saya URL lagu dari **Spotify, Deezer, atau Tidal**.\n"
        "Saya akan mencari kualitas Lossless/FLAC dan mengirimkannya dengan kecepatan tinggi!"
    )

@app.on_message(filters.text & filters.regex(r"https?://(www\.)?(open\.spotify\.com|spotify\.com|deezer\.com|deezer\.page\.link|tidal\.com|song\.link)/"))
async def handle_url(client, message: Message):
    url = message.text
    
    # Hanya kirim satu pesan notifikasi di awal
    status_msg = await message.reply_text("⏳ `Sedang memproses lagumu, mohon tunggu sebentar...`")
    
    downloader = AsyncUniversalDownloader()
    
    async with aiohttp.ClientSession(headers=downloader.headers) as session:
        # Cek apakah URL adalah Playlist Spotify
        if "/playlist/" in url:
            playlist_title, track_urls = await downloader.fetch_spotify_playlist_tracks(session, url)
            if not track_urls:
                await status_msg.delete()
                return await message.reply_text("❌ Gagal mengambil track dari playlist ini.")
                
            await status_msg.edit_text(f"⏳ `Memproses Playlist: {playlist_title}`\n`Ditemukan {len(track_urls)} lagu. Sedang mendownload...`")
            
            downloaded_files = []
            total_size = 0
            MAX_ZIP_SIZE = 1.95 * 1024 * 1024 * 1024 # 1.95 GB limit
            
            for i, track_url in enumerate(track_urls):
                try:
                    meta = await downloader.fetch_spotify_metadata(session, track_url)
                    t_id, d_id = await downloader.resolve_with_songlink(session, track_url)
                    if not t_id: continue
                    
                    if not meta:
                        meta = await downloader.fetch_metadata(session, d_id)
                    elif d_id:
                        d_meta = await downloader.fetch_metadata(session, d_id)
                        if d_meta:
                            if meta.get("title") and meta.get("title") != "Unknown": d_meta["title"] = meta["title"]
                            if meta.get("artist") and meta.get("artist") != "Unknown": d_meta["artist"] = meta["artist"]
                            meta = d_meta
                            
                    api_data = await downloader.fetch_manifest_parallel(session, t_id)
                    if not api_data: continue
                    
                    direct_url = downloader.decode_manifest(api_data)
                    if not direct_url: continue
                    
                    filepath, _ = await downloader.download_file(session, direct_url, meta, t_id)
                    if filepath and os.path.exists(filepath):
                        file_size = os.path.getsize(filepath)
                        
                        if total_size + file_size > MAX_ZIP_SIZE:
                            os.remove(filepath)
                            logger.info(f"Size limit reached. Removing last track to stay under 2GB.")
                            break
                            
                        downloaded_files.append(filepath)
                        total_size += file_size
                except Exception as e:
                    logger.error(f"Gagal download lagu {track_url} di playlist: {e}")
            
            if not downloaded_files:
                await status_msg.delete()
                return await message.reply_text("❌ Gagal mengunduh lagu apa pun dari playlist.")
                
            await status_msg.edit_text("⏳ `Membuat file ZIP, mohon tunggu...`")
            
            zip_filename = f"{downloader.clean_filename(playlist_title)}.zip"
            zip_filepath = os.path.join(downloader.output_dir, zip_filename)
            
            try:
                # Compression method ZIP_STORED (or ZIP_DEFLATED)
                with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_STORED) as zipf:
                    for file in downloaded_files:
                        zipf.write(file, os.path.basename(file))
                
                await status_msg.edit_text("⏳ `Mengunggah file ZIP ke Telegram...`")
                await message.reply_document(
                    document=zip_filepath,
                    caption=f"✅ **{playlist_title}**\n💿 Playlist Download ({len(downloaded_files)} lagu)"
                )
            except Exception as e:
                logger.error(f"Gagal upload zip: {e}")
                await message.reply_text(f"❌ Gagal mengirim file ZIP: {e}")
            finally:
                # Cleanup zip and individual files
                if os.path.exists(zip_filepath):
                    os.remove(zip_filepath)
                for file in downloaded_files:
                    if os.path.exists(file):
                        os.remove(file)
                await status_msg.delete()
            return

        # 1. Ambil Metadata dari Spotify (jika URL Spotify)
        metadata = await downloader.fetch_spotify_metadata(session, url)
        
        # 2. Resolve ID (Cari padanan lintas platform via song.link)
        tidal_id, deezer_id = await downloader.resolve_with_songlink(session, url)
        if not tidal_id:
            await status_msg.delete()
            return await message.reply_text("❌ Gagal menemukan padanan lagu ini di database Lossless.")
            
        # 3. Cek Cache
        cached_entry = audio_cache.get(tidal_id)
        if cached_entry:
            cached_audio = cached_entry["filepath"]
            cached_title = cached_entry["title"]
            cached_performer = cached_entry["performer"]
            
            # Metadata tidak perlu diambil lagi karena file sudah siap dikirim
            try:
                await message.reply_audio(
                    audio=cached_audio,
                    title=cached_title,
                    performer=cached_performer,
                    caption=f"✅ **{cached_performer} - {cached_title}**\n💿 Lossless Download (Cache)"
                )
                await status_msg.delete()
                return
            except Exception as e:
                logger.error(f"Gagal mengirim file fisik dari cache: {e}")
                # Jika gagal, bisa jatuh kembali ke proses normal (download ulang)
                
        # 4. Ambil Metadata Deezer untuk validasi silang (jika Spotify metadata tidak ada)
        if not metadata:
            metadata = await downloader.fetch_metadata(session, deezer_id)
        elif deezer_id:
            # Ambil detail lengkap dari deezer jika kita hanya punya judul dari Spotify
            deezer_meta = await downloader.fetch_metadata(session, deezer_id)
            if deezer_meta:
                # Merge: perbarui deezer_meta dengan data dari spotify jika deezer tidak punya,
                # tapi utamakan deezer_meta untuk isrc, cover, dll.
                if metadata.get("title") and metadata.get("title") != "Unknown":
                    deezer_meta["title"] = metadata["title"]
                if metadata.get("artist") and metadata.get("artist") != "Unknown":
                    deezer_meta["artist"] = metadata["artist"]
                metadata = deezer_meta
        
        # 5. Fan-out Mirror (Paralel)
        api_data = await downloader.fetch_manifest_parallel(session, tidal_id)
        if not api_data:
            await status_msg.delete()
            return await message.reply_text("❌ Gagal mendapatkan source audio dari semua mirror.")
            
        # 5. Decode
        direct_url = downloader.decode_manifest(api_data)
        if not direct_url:
            await status_msg.delete()
            return await message.reply_text("❌ Gagal mendekode link audio.")
            
        # 6. Download (Tanpa edit_text)
        filepath, meta = await downloader.download_file(session, direct_url, metadata, tidal_id)
        
        if not filepath:
            await status_msg.delete()
            return await message.reply_text("❌ Terjadi kesalahan saat mengunduh file.")

    # 7. Upload ke Telegram
    try:
        title = meta['title'] if meta else f"Track {tidal_id}"
        performer = meta['artist'] if meta else "Unknown Artist"
        
        # Kirim audio langsung tanpa parameter progress
        sent_msg = await message.reply_audio(
            audio=filepath,
            title=title,
            performer=performer,
            caption=f"✅ **{performer} - {title}**\n💿 Lossless Download"
        )
        # Hapus pesan "Sedang memproses..." setelah lagu berhasil dikirim
        await status_msg.delete() 
        
        # 8. Simpan info file lokal ke Cache untuk dihapus otomatis dalam 12 jam
        audio_cache.put(tidal_id, filepath, title, performer)
            
    except Exception as e:
        await status_msg.delete()
        await message.reply_text(f"❌ Gagal mengunggah ke Telegram: {e}")
        # Jika gagal upload, kita bisa menghapus file lokal sebagai fallback
        if os.path.exists(filepath):
            os.remove(filepath)

if __name__ == "__main__":
    print("🧹 Membersihkan sisa file cache dari sesi sebelumnya...")
    audio_cache.clear_leftovers()
    print("🚀 Bot sedang berjalan (Mode Cepat & Tanpa Spam)...")
    app.run()
