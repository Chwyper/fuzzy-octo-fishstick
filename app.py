"""Dashboard Telemedicine Big Data: Spark + Neo4j.

Menyediakan antarmuka visual interaktif berbasis Streamlit:
- 1-Click Pipeline Runner (ekstraksi & muat data ke Neo4j)
- Analisis graf & metrik (Gejala terbanyak, gejala penyerta, beban dokter, pasien mirip)
- Visualisasi graf interaktif (PyVis embedded)
- Eksplorasi chat & pengujian NLP ekstraksi gejala (termasuk deteksi negasi)
- Cypher query console & CRUD demo

Jalankan dengan:
    uv run streamlit run app.py
"""
import json
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

# Tambahkan src ke path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from common import (
    CHATS_PATH,
    GRAPH_HTML,
    NEO4J_DB,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    get_driver,
    init_schema,
    run_query,
)
from symptoms import PATTERNS, SYMPTOM_KEYWORDS

# Konfigurasi Halaman Streamlit
st.set_page_config(
    page_title="Telemed Big Data Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1e3a8a;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #4b5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #f8fafc;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #e2e8f0;
        text-align: center;
    }
    .chat-bubble-patient {
        background-color: #e0f2fe;
        color: #0369a1;
        padding: 10px 14px;
        border-radius: 12px 12px 12px 2px;
        margin-bottom: 8px;
        max-width: 80%;
    }
    .chat-bubble-doctor {
        background-color: #f1f5f9;
        color: #334155;
        padding: 10px 14px;
        border-radius: 12px 12px 2px 12px;
        margin-bottom: 8px;
        margin-left: auto;
        max-width: 80%;
        text-align: right;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def check_neo4j_connection():
    """Periksa status koneksi ke Neo4j."""
    try:
        driver = get_driver()
        with driver.session(database=NEO4J_DB) as session:
            session.run("RETURN 1").single()
        return True, "Terhubung"
    except Exception as e:
        return False, str(e)


def extract_symptoms_python(text: str) -> list[str]:
    """Ekstraksi gejala dari teks berbasis regex dan kamus (dengan negasi)."""
    text_lower = text.lower()
    found = []
    for name, pola in PATTERNS.items():
        if re.search(pola, text_lower):
            found.append(name)
    return found


def execute_pipeline_runner(data_path: str):
    """Jalankan proses ingestion & loading ke Neo4j."""
    driver = get_driver()
    init_schema(driver)

    t0 = time.time()
    chat_items = []
    if os.path.isdir(data_path):
        for f in Path(data_path).glob("*.json"):
            with open(f, "r", encoding="utf-8") as fp:
                for line in fp:
                    if line.strip():
                        chat_items.append(json.loads(line.strip()))
    else:
        with open(data_path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
            chat_items = data if isinstance(data, list) else [data]

    rows = []
    for c in chat_items:
        cid = c.get("chat_id")
        pid = c.get("patient_id")
        did = c.get("doctor_id")
        ts = c.get("timestamp", "")
        for m in c.get("messages", []):
            if m.get("from") == "patient":
                symptoms = extract_symptoms_python(m.get("text", ""))
                for sym in symptoms:
                    rows.append({
                        "chat_id": cid,
                        "patient_id": pid,
                        "doctor_id": did,
                        "timestamp": ts,
                        "gejala": sym,
                    })

    # Deduplikasi baris per chat
    rows = [dict(t) for t in {tuple(sorted(d.items())) for d in rows}]

    LOAD_QUERY = """
    UNWIND $rows AS event
    MERGE (p:Pasien {id: event.patient_id})
    MERGE (d:Dokter {id: event.doctor_id})
    MERGE (g:Gejala {nama: event.gejala})
    MERGE (p)-[:DIPERIKSA_OLEH]->(d)
    MERGE (p)-[m:MENGALAMI {chat_id: event.chat_id}]->(g)
    SET m.waktu = event.timestamp
    """
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        run_query(driver, LOAD_QUERY, rows=rows[i:i + batch_size])

    elapsed = time.time() - t0
    driver.close()
    return len(rows), len(chat_items), elapsed


def generate_pyvis_html(driver, max_relations=300):
    """Buat file graph.html dan kembalikan konten HTML string."""
    from pyvis.network import Network

    net = Network(height="650px", width="100%", directed=True, bgcolor="#ffffff",
                  notebook=False, cdn_resources="in_line")
    warna = {"Pasien": "#3b82f6", "Gejala": "#ef4444", "Dokter": "#10b981"}

    rel = run_query(
        driver,
        f"""
        MATCH (a)-[r]->(b)
        RETURN labels(a)[0] AS la, coalesce(a.id, a.nama) AS a, type(r) AS tipe,
               labels(b)[0] AS lb, coalesce(b.id, b.nama) AS b
        LIMIT {max_relations}
        """,
    )
    for r in rel:
        for label, nama in ((r["la"], r["a"]), (r["lb"], r["b"])):
            c = warna.get(label, "#94a3b8")
            net.add_node(f"{label}:{nama}", label=str(nama), color=c, title=label, shape="dot", size=18)
        net.add_edge(f"{r['la']}:{r['a']}", f"{r['lb']}:{r['b']}", title=r["tipe"], color="#cbd5e1")

    html_content = net.generate_html()
    with open(GRAPH_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)
    return html_content, len(rel)


# ================= SIDEBAR =================
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2966/2966327.png", width=65)
    st.title("Telemed Big Data")
    st.caption("Pola: Ingestion → Processing → NoSQL (Neo4j) → Analytics")

    st.markdown("---")
    st.subheader("🔌 Status Neo4j")
    is_connected, msg = check_neo4j_connection()
    if is_connected:
        st.success("🟢 Neo4j Connected (`bolt://localhost:7687`)")
    else:
        st.error(f"🔴 Neo4j Terputus: {msg}")
        st.info("Pastikan Docker Desktop aktif lalu jalankan:\n`docker compose up -d`")

    st.markdown("---")
    st.subheader("⚡ 1-Click Pipeline Ingestion")
    source_choice = st.selectbox(
        "Pilih Sumber Chat:",
        options=["data/chats.json (12 Data Demo)", "data/synthetic (100k Sintetis)"],
        index=0,
    )
    chosen_path = CHATS_PATH if "chats.json" in source_choice else str(ROOT / "data" / "synthetic")

    if st.button("🚀 Jalankan Pipeline Sekarang", use_container_width=True, type="primary"):
        if not is_connected:
            st.error("Gagal: Database Neo4j belum aktif.")
        else:
            with st.spinner("Memproses data & memuat ke Neo4j..."):
                try:
                    rel_count, chat_count, dur = execute_pipeline_runner(chosen_path)
                    st.success(f"✅ Berhasil memproses {chat_count} chat ({rel_count} relasi) dalam {dur:.2f} detik!")
                    time.sleep(1)
                    st.rerun()
                except Exception as ex:
                    st.error(f"Terjadi kesalahan: {ex}")

    if st.button("🗑️ Reset Graf (Kosongkan Data)", use_container_width=True):
        if is_connected:
            driver = get_driver()
            run_query(driver, "MATCH (n) DETACH DELETE n")
            driver.close()
            st.warning("Data di Neo4j telah dikosongkan.")
            time.sleep(0.5)
            st.rerun()


# ================= MAIN CONTENT =================
st.markdown('<div class="main-title">🏥 Telemedicine Big Data & Graph Analytics</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">Pipeline Big Data terpadu: Ekstraksi gejala konsultasi pasien menggunakan Spark & penyimpanan graf relasi Neo4j</div>',
    unsafe_allow_html=True,
)

if not is_connected:
    st.warning("⚠️ Database Neo4j belum menyala. Silakan buka **Docker Desktop** dan jalankan `docker compose up -d` di terminal untuk melihat data graf.")

# Ambil data metrik dari Neo4j jika terhubung
stats = {"total_pasien": 0, "total_dokter": 0, "total_gejala": 0, "total_mengalami": 0}
if is_connected:
    try:
        driver = get_driver()
        q_stats = run_query(
            driver,
            """
            MATCH (p:Pasien) WITH count(p) as tp
            MATCH (d:Dokter) WITH tp, count(d) as td
            MATCH (g:Gejala) WITH tp, td, count(g) as tg
            MATCH ()-[m:MENGALAMI]->() WITH tp, td, tg, count(m) as tm
            RETURN tp, td, tg, tm
            """,
        )
        if q_stats:
            stats["total_pasien"] = q_stats[0]["tp"]
            stats["total_dokter"] = q_stats[0]["td"]
            stats["total_gejala"] = q_stats[0]["tg"]
            stats["total_mengalami"] = q_stats[0]["tm"]
        driver.close()
    except Exception:
        pass

# KPI Cards
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Pasien", f"{stats['total_pasien']:,}", help="Node dengan label :Pasien")
c2.metric("Total Dokter", f"{stats['total_dokter']:,}", help="Node dengan label :Dokter")
c3.metric("Total Gejala Unik", f"{stats['total_gejala']:,}", help="Node dengan label :Gejala")
c4.metric("Relasi Gejala (MENGALAMI)", f"{stats['total_mengalami']:,}", help="Edge [:MENGALAMI]")

st.write("")

# Tabs
tab_analytics, tab_graph, tab_chat, tab_crud = st.tabs([
    "📊 Analitik & Grafik",
    "🕸️ Visualisasi Graf Interaktif",
    "💬 Eksplorasi Chat & NLP",
    "⚡ Cypher Console & CRUD",
])

# ================= TAB 1: ANALITIK =================
with tab_analytics:
    if not is_connected or stats["total_pasien"] == 0:
        st.info("Klik tombol **'🚀 Jalankan Pipeline Sekarang'** di panel kiri untuk memuat data ke dalam Neo4j.")
    else:
        driver = get_driver()

        col_left, col_right = st.columns(2)

        # 1. Gejala Terbanyak
        with col_left:
            st.subheader("📈 Gejala Paling Sering Dialami")
            top_gejala = run_query(
                driver,
                """
                MATCH (:Pasien)-[m:MENGALAMI]->(g:Gejala)
                RETURN g.nama AS gejala, count(m) AS jumlah_kasus
                ORDER BY jumlah_kasus DESC
                LIMIT 10
                """,
            )
            if top_gejala:
                df_top = pd.DataFrame(top_gejala)
                fig_top = px.bar(
                    df_top,
                    x="jumlah_kasus",
                    y="gejala",
                    orientation="h",
                    color="jumlah_kasus",
                    color_continuous_scale="Reds",
                    text="jumlah_kasus",
                )
                fig_top.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False, height=350)
                st.plotly_chart(fig_top, use_container_width=True)
            else:
                st.write("Belum ada data gejala.")

        # 2. Beban Dokter
        with col_right:
            st.subheader("👨‍⚕️ Distribusi Beban Pasien per Dokter")
            beban_dokter = run_query(
                driver,
                """
                MATCH (d:Dokter)<-[:DIPERIKSA_OLEH]-(p:Pasien)
                RETURN d.id AS dokter, count(DISTINCT p) AS jumlah_pasien
                ORDER BY jumlah_pasien DESC
                """,
            )
            if beban_dokter:
                df_beban = pd.DataFrame(beban_dokter)
                fig_beban = px.pie(
                    df_beban,
                    values="jumlah_pasien",
                    names="dokter",
                    hole=0.45,
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_beban.update_layout(height=350)
                st.plotly_chart(fig_beban, use_container_width=True)
            else:
                st.write("Belum ada data dokter.")

        st.markdown("---")

        # 3. Gejala Penyerta (Co-morbidity)
        st.subheader("🔍 Analisis Gejala Penyerta (Co-occurrence Pattern)")
        st.caption("Menganalisis gejala yang muncul bersamaan dalam satu sesi konsultasi (Pola Graf: `(g1)<-[:MENGALAMI]-(p)-[:MENGALAMI]->(g2)`)")

        all_gejala = run_query(driver, "MATCH (g:Gejala) RETURN g.nama AS nama ORDER BY nama")
        symptom_list = [g["nama"] for g in all_gejala]

        if symptom_list:
            selected_symptom = st.selectbox("Pilih Gejala Utama:", options=symptom_list, index=0)
            penyerta = run_query(
                driver,
                """
                MATCH (g1:Gejala {nama: $nama})<-[m1:MENGALAMI]-(:Pasien)-[m2:MENGALAMI]->(g2:Gejala)
                WHERE m1.chat_id = m2.chat_id AND g1 <> g2
                RETURN g2.nama AS gejala_penyerta, count(DISTINCT m1.chat_id) AS frekuensi
                ORDER BY frekuensi DESC, gejala_penyerta
                """,
                nama=selected_symptom,
            )
            if penyerta:
                df_penyerta = pd.DataFrame(penyerta)
                c_p1, c_p2 = st.columns([2, 1])
                with c_p1:
                    fig_p = px.bar(
                        df_penyerta,
                        x="gejala_penyerta",
                        y="frekuensi",
                        color="frekuensi",
                        color_continuous_scale="Purples",
                        text="frekuensi",
                    )
                    fig_p.update_layout(height=300)
                    st.plotly_chart(fig_p, use_container_width=True)
                with c_p2:
                    st.dataframe(df_penyerta, use_container_width=True, hide_index=True)
            else:
                st.info(f"Tidak ada gejala penyerta yang tercatat bersamaan dengan '{selected_symptom}'.")

        st.markdown("---")

        # 4. Pasien Mirip (Similar Patients - 2 Hop Traversal)
        st.subheader("👥 Penelusuran Pasien Serupa (Graph 2-Hop Traversal)")
        st.caption("Menemukan pasien lain yang memiliki kemiripan gejala dengan pasien target.")
        all_patients = run_query(driver, "MATCH (p:Pasien) RETURN p.id AS id ORDER BY id LIMIT 50")
        patient_list = [p["id"] for p in all_patients]

        if patient_list:
            selected_patient = st.selectbox("Pilih ID Pasien:", options=patient_list, index=0)
            sim_patients = run_query(
                driver,
                """
                MATCH (:Pasien {id: $pid})-[:MENGALAMI]->(g:Gejala)<-[:MENGALAMI]-(lain:Pasien)
                WHERE lain.id <> $pid
                RETURN lain.id AS pasien_lain, collect(DISTINCT g.nama) AS gejala_sama,
                       count(DISTINCT g) AS jumlah_gejala_sama
                ORDER BY jumlah_gejala_sama DESC, pasien_lain
                """,
                pid=selected_patient,
            )
            if sim_patients:
                df_sim = pd.DataFrame(sim_patients)
                df_sim["gejala_sama"] = df_sim["gejala_sama"].apply(lambda x: ", ".join(x))
                st.dataframe(df_sim, use_container_width=True, hide_index=True)
            else:
                st.info(f"Belum ada pasien lain yang memiliki gejala sama dengan {selected_patient}.")

        driver.close()

# ================= TAB 2: VISUALISASI GRAF =================
with tab_graph:
    st.subheader("🕸️ Visualisasi Graf Interaktif")
    st.caption("Representasi graf interaktif node & relasi: 🔵 Pasien, 🔴 Gejala, 🟢 Dokter")

    if not is_connected or stats["total_pasien"] == 0:
        st.info("Graf masih kosong atau Neo4j belum terhubung.")
    else:
        col_g1, col_g2 = st.columns([1, 4])
        with col_g1:
            max_nodes = st.slider("Batas Jumlah Relasi:", min_value=50, max_value=800, value=250, step=50)
            if st.button("🔄 Refresh Graf"):
                st.rerun()

            st.markdown(
                """
                **Keterangan Warna:**
                - <span style="color:#3b82f6; font-weight:bold;">● Pasien</span>
                - <span style="color:#ef4444; font-weight:bold;">● Gejala</span>
                - <span style="color:#10b981; font-weight:bold;">● Dokter</span>
                
                *Klik dan drag node untuk melihat simulasi fisika.*
                """,
                unsafe_allow_html=True,
            )

        with col_g2:
            try:
                driver = get_driver()
                html_content, rel_count = generate_pyvis_html(driver, max_relations=max_nodes)
                driver.close()
                st.caption(f"Menampilkan {rel_count} relasi aktif:")
                components.html(html_content, height=670, scrolling=True)
            except Exception as e_graph:
                st.error(f"Gagal memuat graf interaktif: {e_graph}")

# ================= TAB 3: EKSPLORASI CHAT & NLP =================
with tab_chat:
    st.subheader("💬 Eksplorasi Percakapan & Ekstraksi NLP")
    st.caption("Melihat transkrip obrolan pasien-dokter asli dan bagaimana algoritma mengekstrak gejala (dengan penanganan negasi).")

    subtab_view, subtab_test = st.tabs(["📄 Lihat Transkrip Demo (chats.json)", "🧪 Uji Coba Teks Bebas (Live Tester)"])

    with subtab_view:
        try:
            with open(CHATS_PATH, "r", encoding="utf-8") as f:
                chat_data = json.load(f)

            chat_options = [f"{c['chat_id']} ({c['patient_id']} ➔ {c['doctor_id']})" for c in chat_data]
            sel_chat = st.selectbox("Pilih Sesi Konsultasi:", chat_options)
            sel_idx = chat_options.index(sel_chat)
            active_chat = chat_data[sel_idx]

            col_c1, col_c2 = st.columns([3, 2])
            with col_c1:
                st.write(f"**Chat ID:** `{active_chat['chat_id']}` | **Waktu:** `{active_chat.get('timestamp')}`")
                st.write(f"**Pasien:** `{active_chat['patient_id']}` | **Dokter:** `{active_chat['doctor_id']}`")
                st.markdown("---")

                extracted_all = []
                for msg in active_chat.get("messages", []):
                    sender = msg.get("from")
                    text = msg.get("text", "")
                    if sender == "patient":
                        syms = extract_symptoms_python(text)
                        extracted_all.extend(syms)
                        sym_badge = f"<br><small><b>Gejala terdeteksi:</b> {', '.join(syms)}</small>" if syms else ""
                        st.markdown(f'<div class="chat-bubble-patient"><b>Pasien:</b> {text}{sym_badge}</div>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="chat-bubble-doctor"><b>Dokter:</b> {text}</div>', unsafe_allow_html=True)

            with col_c2:
                st.markdown("#### 🎯 Hasil Ekstraksi untuk Neo4j")
                unique_syms = sorted(list(set(extracted_all)))
                if unique_syms:
                    st.success(f"Ditemukan {len(unique_syms)} gejala unik:")
                    for s in unique_syms:
                        st.markdown(f"- 🩺 **{s}**")
                    st.info(f"Relasi yang disimpan di graf:\n\n`(:Pasien {{id: '{active_chat['patient_id']}'}})-[:MENGALAMI]->(:Gejala)`")
                else:
                    st.warning("Tidak ada gejala yang teridentifikasi dari pesan pasien.")
        except Exception as e_chat:
            st.error(f"Gagal membaca chats.json: {e_chat}")

    with subtab_test:
        st.markdown("#### 🧪 Sandbox Ekstraksi Gejala & Deteksi Negasi")
        st.write("Ketik kalimat keluhan medis untuk menguji apakah sistem berhasil mengekstrak gejala dan mengabaikan gejala yang dinegasi (misal: 'tidak batuk', 'tanpa demam'):")

        sample_input = st.text_area(
            "Masukkan Kalimat Keluhan:",
            value="Halo dok, kepala saya terasa pusing dan badan lemas sejak semalam. Tapi saya tidak demam dan tanpa sesak napas.",
            height=100,
        )

        if sample_input:
            detected = extract_symptoms_python(sample_input)
            col_res1, col_res2 = st.columns(2)
            with col_res1:
                st.markdown("**Hasil Deteksi Gejala Positif:**")
                if detected:
                    for d in detected:
                        st.success(f"✅ {d.upper()}")
                else:
                    st.info("Tidak ada gejala positif yang terdeteksi.")

            with col_res2:
                st.markdown("**Penjelasan Logika Veracity (Negasi):**")
                st.caption(
                    "Sistem menggunakan regex lookbehind `(?<!tidak )(?<!tidak ada )(?<!tanpa )(?<!belum ada )`. "
                    "Sehingga kata 'demam' yang didahului 'tidak' secara otomatis diabaikan dan tidak dimasukkan ke dalam basis data graf."
                )

# ================= TAB 4: CYPHER CONSOLE & CRUD =================
with tab_crud:
    st.subheader("⚡ Cypher Query Console & CRUD")
    st.caption("Jalankan query analitik kustom langsung ke Neo4j atau demonstrasikan operasi CRUD (Create, Read, Update, Delete).")

    if not is_connected:
        st.warning("Neo4j belum terhubung.")
    else:
        st.markdown("#### 💻 Eksekusi Query Cypher")
        preset = st.selectbox(
            "Pilih Contoh Query Cepat:",
            options=[
                "Kustom (Tulis Sendiri)",
                "MATCH (p:Pasien)-[m:MENGALAMI]->(g:Gejala) RETURN p.id AS pasien, g.nama AS gejala, m.chat_id AS chat_id LIMIT 25",
                "MATCH (d:Dokter)<-[:DIPERIKSA_OLEH]-(p:Pasien) RETURN d.id AS dokter, collect(p.id) AS daftar_pasien",
                "MATCH (g:Gejala) RETURN g.nama AS gejala, COUNT { (g)<--() } AS relasi_masuk ORDER BY relasi_masuk DESC",
            ],
        )

        query_input = st.text_area(
            "Query Cypher:",
            value=preset if preset != "Kustom (Tulis Sendiri)" else "MATCH (p:Pasien)-[r]->(b) RETURN p.id AS pasien, type(r) AS relasi, coalesce(b.nama, b.id) AS target LIMIT 20",
            height=110,
        )

        if st.button("▶️ Jalankan Query"):
            try:
                driver = get_driver()
                res = run_query(driver, query_input)
                driver.close()
                if res:
                    st.success(f"Ditemukan {len(res)} hasil:")
                    st.dataframe(pd.DataFrame(res), use_container_width=True)
                else:
                    st.info("Query berhasil dijalankan, namun tidak ada baris data yang dikembalikan.")
            except Exception as e_query:
                st.error(f"Error saat mengeksekusi Cypher: {e_query}")

        st.markdown("---")
        st.markdown("#### 📝 Demo Operasi CRUD Interaktif (Lab NoSQL)")

        c_crud1, c_crud2 = st.columns(2)
        with c_crud1:
            st.markdown("**1. Tambah Data Pasien Baru (CREATE)**")
            new_pid = st.text_input("ID Pasien Baru:", value="P_DEMO1")
            new_age = st.number_input("Usia Pasien:", min_value=1, max_value=120, value=28)
            new_sym = st.selectbox("Gejala:", options=list(SYMPTOM_KEYWORDS.keys()))
            if st.button("➕ Simpan ke Graf"):
                driver = get_driver()
                run_query(
                    driver,
                    """
                    MERGE (p:Pasien {id: $pid}) SET p.usia = $usia
                    MERGE (g:Gejala {nama: $gejala})
                    MERGE (p)-[:MENGALAMI {chat_id: 'DEMO_CRUD'}]->(g)
                    """,
                    pid=new_pid,
                    usia=new_age,
                    gejala=new_sym,
                )
                driver.close()
                st.success(f"Pasien {new_pid} berhasil ditambahkan dengan gejala {new_sym}!")
                time.sleep(0.5)
                st.rerun()

        with c_crud2:
            st.markdown("**2. Hapus Pasien (DELETE / DETACH DELETE)**")
            del_pid = st.text_input("ID Pasien yang Dihapus:", value="P_DEMO1")
            if st.button("🗑️ Hapus Pasien"):
                driver = get_driver()
                run_query(driver, "MATCH (p:Pasien {id: $pid}) DETACH DELETE p", pid=del_pid)
                driver.close()
                st.warning(f"Pasien {del_pid} dan seluruh relasinya telah dihapus.")
                time.sleep(0.5)
                st.rerun()
