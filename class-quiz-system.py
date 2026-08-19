from fastapi import FastAPI, HTTPException, Depends, Cookie, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import sqlite3
import hashlib
import secrets
import time
import csv
import io
import asyncio
import uvicorn

# ---------------- basic config (just hardcoding stuff here, easier for now) ----------------

DB_FILE = "classquiz.db"
HOST = "127.0.0.1"
PORT = 8000
SESSION_TIME = 24 * 60 * 60  # 1 day in seconds

app = FastAPI(title="ClassQuiz")

# sessions live in memory, not in db. simpler and works fine for now
# format: { session_id: {id, name, username, role, expires} }
SESSIONS = {}

# websocket clients per quiz, quiz_id -> list of websocket objects
LIVE_CLIENTS = {}


# ---------------- db helpers ----------------

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def setup_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            username TEXT UNIQUE,
            password_hash TEXT,
            role TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quizzes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            teacher_id INTEGER,
            title TEXT,
            description TEXT,
            duration_seconds INTEGER,
            negative_marking REAL,
            status TEXT DEFAULT 'draft',
            start_time INTEGER,
            end_time INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_id INTEGER,
            question_text TEXT,
            marks REAL,
            position INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question_id INTEGER,
            option_text TEXT,
            is_correct INTEGER,
            position INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_id INTEGER,
            student_id INTEGER,
            started_at INTEGER,
            submitted_at INTEGER,
            status TEXT DEFAULT 'active',
            score REAL DEFAULT 0,
            correct_count INTEGER DEFAULT 0,
            wrong_count INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attempt_id INTEGER,
            question_id INTEGER,
            option_id INTEGER
        )
    """)

    conn.commit()
    conn.close()


# ---------------- password stuff ----------------
# not using anything fancy like bcrypt, just sha256 + a random salt

def hash_password(password):
    salt = secrets.token_hex(8)
    digest = hashlib.sha256((salt + password).encode()).hexdigest()
    return salt + "$" + digest


def check_password(password, stored_hash):
    try:
        salt, digest = stored_hash.split("$")
        return hashlib.sha256((salt + password).encode()).hexdigest() == digest
    except:
        return False


def make_session(user_row):
    session_id = secrets.token_hex(16)
    SESSIONS[session_id] = {
        "id": user_row["id"],
        "name": user_row["name"],
        "username": user_row["username"],
        "role": user_row["role"],
        "expires": time.time() + SESSION_TIME,
    }
    return session_id


def get_current_user(classquiz_session: str = Cookie(None)):
    if classquiz_session is None or classquiz_session not in SESSIONS:
        raise HTTPException(status_code=401, detail="please login first")

    user = SESSIONS[classquiz_session]

    if time.time() > user["expires"]:
        del SESSIONS[classquiz_session]
        raise HTTPException(status_code=401, detail="session expired, login again")

    return user


def teacher_only(user=Depends(get_current_user)):
    if user["role"] != "teacher":
        raise HTTPException(status_code=403, detail="teachers only")
    return user


def student_only(user=Depends(get_current_user)):
    if user["role"] != "student":
        raise HTTPException(status_code=403, detail="students only")
    return user


# ---------------- seed some demo accounts so its easy to test ----------------

def add_user_if_new(name, username, password, role):
    conn = get_db()
    cur = conn.cursor()

    existing = cur.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
    if existing:
        conn.close()
        return

    cur.execute(
        "INSERT INTO users (name, username, password_hash, role) VALUES (?,?,?,?)",
        (name, username, hash_password(password), role),
    )
    conn.commit()
    conn.close()


def seed_users():
    add_user_if_new("Demo Teacher", "teacher", "teacher123", "teacher")
    for i in range(1, 51):
        uname = f"student{i:03d}"
        add_user_if_new(f"Student {i:03d}", uname, "student123", "student")


# ---------------- request models (kept simple, no fancy validators) ----------------

class LoginData(BaseModel):
    username: str
    password: str


class QuizData(BaseModel):
    title: str
    description: str = ""
    duration_seconds: int = 60
    negative_marking: float = 0


class QuestionData(BaseModel):
    question_text: str
    options: list[str]
    correct_index: int
    marks: float = 1


class AnswerData(BaseModel):
    question_id: int
    option_id: int


# ---------------- small helper functions ----------------

def now():
    return int(time.time())


def get_quiz_or_404(quiz_id):
    conn = get_db()
    quiz = conn.execute("SELECT * FROM quizzes WHERE id=?", (quiz_id,)).fetchone()
    conn.close()
    if not quiz:
        raise HTTPException(status_code=404, detail="quiz not found")
    return quiz


def get_questions(quiz_id, with_answers=False):
    conn = get_db()
    q_rows = conn.execute(
        "SELECT * FROM questions WHERE quiz_id=? ORDER BY position", (quiz_id,)
    ).fetchall()

    result = []
    for q in q_rows:
        opt_rows = conn.execute(
            "SELECT * FROM options WHERE question_id=? ORDER BY position", (q["id"],)
        ).fetchall()

        opts = []
        for o in opt_rows:
            item = {"id": o["id"], "text": o["option_text"], "position": o["position"]}
            if with_answers:
                item["is_correct"] = bool(o["is_correct"])
            opts.append(item)

        result.append({
            "id": q["id"],
            "question_text": q["question_text"],
            "marks": q["marks"],
            "position": q["position"],
            "options": opts,
        })

    conn.close()
    return result


def get_stats(quiz_id):
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM attempts WHERE quiz_id=?", (quiz_id,)).fetchone()[0]
    submitted = conn.execute(
        "SELECT COUNT(*) FROM attempts WHERE quiz_id=? AND status='submitted'", (quiz_id,)
    ).fetchone()[0]
    conn.close()

    return {
        "participants": total,
        "submitted": submitted,
        "not_submitted": total - submitted,
    }


async def broadcast_stats(quiz_id):
    stats = get_stats(quiz_id)
    clients = LIVE_CLIENTS.get(quiz_id, [])

    dead = []
    for ws in clients:
        try:
            await ws.send_json({"type": "stats", **stats})
        except:
            dead.append(ws)

    for ws in dead:
        if ws in clients:
            clients.remove(ws)


def grade_attempt(attempt_id):
    conn = get_db()
    cur = conn.cursor()

    attempt = cur.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
    if not attempt:
        conn.close()
        return None

    quiz = cur.execute("SELECT * FROM quizzes WHERE id=?", (attempt["quiz_id"],)).fetchone()

    ans_rows = cur.execute(
        "SELECT * FROM answers WHERE attempt_id=?", (attempt_id,)
    ).fetchall()

    correct = 0
    wrong = 0
    score = 0.0

    for a in ans_rows:
        opt = cur.execute("SELECT * FROM options WHERE id=?", (a["option_id"],)).fetchone()
        q = cur.execute("SELECT * FROM questions WHERE id=?", (a["question_id"],)).fetchone()

        if opt and opt["is_correct"]:
            correct += 1
            score += q["marks"]
        else:
            wrong += 1
            score -= quiz["negative_marking"]

    score = round(score, 2)

    if attempt["status"] != "submitted":
        cur.execute(
            "UPDATE attempts SET status='submitted', submitted_at=?, score=?, correct_count=?, wrong_count=? WHERE id=?",
            (now(), score, correct, wrong, attempt_id),
        )
        conn.commit()

    conn.close()
    return {"score": score, "correct": correct, "wrong": wrong}


# ---------------- auth routes ----------------

@app.post("/api/login")
def login(data: LoginData):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username=?", (data.username,)).fetchone()
    conn.close()

    if not user or not check_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="wrong username or password")

    session_id = make_session(user)

    return {
        "session_id": session_id,
        "user": {
            "id": user["id"],
            "name": user["name"],
            "username": user["username"],
            "role": user["role"],
        },
    }


@app.post("/api/logout")
def logout(classquiz_session: str = Cookie(None)):
    if classquiz_session in SESSIONS:
        del SESSIONS[classquiz_session]
    return {"success": True}


@app.get("/api/me")
def me(user=Depends(get_current_user)):
    return user


# ---------------- teacher: create quiz + questions ----------------

@app.post("/api/quizzes")
def create_quiz(data: QuizData, teacher=Depends(teacher_only)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO quizzes (teacher_id, title, description, duration_seconds, negative_marking) VALUES (?,?,?,?,?)",
        (teacher["id"], data.title, data.description, data.duration_seconds, data.negative_marking),
    )
    quiz_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {"quiz_id": quiz_id, "message": "quiz created"}


@app.post("/api/quizzes/{quiz_id}/questions")
def add_question(quiz_id: int, data: QuestionData, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)

    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    if quiz["status"] != "draft":
        raise HTTPException(status_code=400, detail="can only add questions to draft quizzes")

    if data.correct_index >= len(data.options):
        raise HTTPException(status_code=400, detail="correct_index is out of range")

    conn = get_db()
    cur = conn.cursor()

    position = cur.execute(
        "SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz_id,)
    ).fetchone()[0]

    cur.execute(
        "INSERT INTO questions (quiz_id, question_text, marks, position) VALUES (?,?,?,?)",
        (quiz_id, data.question_text, data.marks, position),
    )
    question_id = cur.lastrowid

    for i, opt_text in enumerate(data.options):
        cur.execute(
            "INSERT INTO options (question_id, option_text, is_correct, position) VALUES (?,?,?,?)",
            (question_id, opt_text, 1 if i == data.correct_index else 0, i),
        )

    conn.commit()
    conn.close()

    return {"question_id": question_id, "message": "question added"}


@app.get("/api/teacher/quizzes")
def teacher_quizzes(teacher=Depends(teacher_only)):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM quizzes WHERE teacher_id=? ORDER BY id DESC", (teacher["id"],)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/quizzes/{quiz_id}")
def quiz_details(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    return {"quiz": dict(quiz), "questions": get_questions(quiz_id, with_answers=True)}


# ---------------- start / stop ----------------

@app.post("/api/quizzes/{quiz_id}/start")
async def start_quiz(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    if quiz["status"] == "live":
        raise HTTPException(status_code=400, detail="already live")

    questions = get_questions(quiz_id)
    if len(questions) == 0:
        raise HTTPException(status_code=400, detail="add at least one question first")

    start_time = now()
    end_time = start_time + quiz["duration_seconds"]

    conn = get_db()
    conn.execute(
        "UPDATE quizzes SET status='live', start_time=?, end_time=? WHERE id=?",
        (start_time, end_time, quiz_id),
    )
    conn.commit()
    conn.close()

    await broadcast_stats(quiz_id)

    return {"success": True, "start_time": start_time, "end_time": end_time}


@app.post("/api/quizzes/{quiz_id}/stop")
async def stop_quiz(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    conn = get_db()
    conn.execute("UPDATE quizzes SET status='stopped' WHERE id=?", (quiz_id,))

    active_attempts = conn.execute(
        "SELECT id FROM attempts WHERE quiz_id=? AND status='active'", (quiz_id,)
    ).fetchall()
    conn.commit()
    conn.close()

    for a in active_attempts:
        grade_attempt(a["id"])

    clients = LIVE_CLIENTS.get(quiz_id, [])
    for ws in clients[:]:
        try:
            await ws.send_json({"type": "quiz_stopped"})
        except:
            pass

    await broadcast_stats(quiz_id)

    return {"success": True}


# ---------------- student: join / answer / submit ----------------

@app.post("/api/quizzes/{quiz_id}/join")
async def join_quiz(quiz_id: int, student=Depends(student_only)):
    quiz = get_quiz_or_404(quiz_id)

    if quiz["status"] != "live":
        raise HTTPException(status_code=400, detail="quiz is not live right now")

    if quiz["end_time"] and now() >= quiz["end_time"]:
        raise HTTPException(status_code=400, detail="quiz time is over")

    conn = get_db()
    cur = conn.cursor()

    attempt = cur.execute(
        "SELECT * FROM attempts WHERE quiz_id=? AND student_id=?", (quiz_id, student["id"])
    ).fetchone()

    if not attempt:
        cur.execute(
            "INSERT INTO attempts (quiz_id, student_id, started_at, status) VALUES (?,?,?,'active')",
            (quiz_id, student["id"], now()),
        )
        attempt_id = cur.lastrowid
        conn.commit()
        attempt = cur.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()

    conn.close()

    await broadcast_stats(quiz_id)

    return {
        "quiz": dict(quiz),
        "attempt": dict(attempt),
        "questions": get_questions(quiz_id),
        "server_time": now(),
    }


@app.post("/api/quizzes/{quiz_id}/answer")
def save_answer(quiz_id: int, data: AnswerData, student=Depends(student_only)):
    quiz = get_quiz_or_404(quiz_id)

    conn = get_db()
    cur = conn.cursor()

    attempt = cur.execute(
        "SELECT * FROM attempts WHERE quiz_id=? AND student_id=?", (quiz_id, student["id"])
    ).fetchone()

    if not attempt:
        conn.close()
        raise HTTPException(status_code=400, detail="join the quiz first")

    if attempt["status"] == "submitted":
        conn.close()
        raise HTTPException(status_code=400, detail="already submitted")

    if quiz["status"] != "live" or (quiz["end_time"] and now() >= quiz["end_time"]):
        conn.close()
        raise HTTPException(status_code=400, detail="quiz is over")

    # basic checks that question/option actually belong to this quiz
    q = cur.execute(
        "SELECT * FROM questions WHERE id=? AND quiz_id=?", (data.question_id, quiz_id)
    ).fetchone()
    if not q:
        conn.close()
        raise HTTPException(status_code=400, detail="invalid question")

    opt = cur.execute(
        "SELECT * FROM options WHERE id=? AND question_id=?", (data.option_id, data.question_id)
    ).fetchone()
    if not opt:
        conn.close()
        raise HTTPException(status_code=400, detail="invalid option")

    existing = cur.execute(
        "SELECT * FROM answers WHERE attempt_id=? AND question_id=?",
        (attempt["id"], data.question_id),
    ).fetchone()

    if existing:
        cur.execute(
            "UPDATE answers SET option_id=? WHERE id=?", (data.option_id, existing["id"])
        )
    else:
        cur.execute(
            "INSERT INTO answers (attempt_id, question_id, option_id) VALUES (?,?,?)",
            (attempt["id"], data.question_id, data.option_id),
        )

    conn.commit()
    conn.close()

    return {"success": True, "server_time": now()}


@app.post("/api/quizzes/{quiz_id}/submit")
async def submit_quiz(quiz_id: int, student=Depends(student_only)):
    conn = get_db()
    attempt = conn.execute(
        "SELECT * FROM attempts WHERE quiz_id=? AND student_id=?", (quiz_id, student["id"])
    ).fetchone()
    conn.close()

    if not attempt:
        raise HTTPException(status_code=404, detail="you haven't joined this quiz")

    result = grade_attempt(attempt["id"])

    await broadcast_stats(quiz_id)

    return {"success": True, **result}


@app.get("/api/quizzes/{quiz_id}/result")
def student_result(quiz_id: int, student=Depends(student_only)):
    conn = get_db()
    attempt = conn.execute(
        "SELECT * FROM attempts WHERE quiz_id=? AND student_id=?", (quiz_id, student["id"])
    ).fetchone()

    if not attempt:
        conn.close()
        raise HTTPException(status_code=404, detail="no attempt found")

    quiz = conn.execute("SELECT * FROM quizzes WHERE id=?", (quiz_id,)).fetchone()

    total_marks = conn.execute(
        "SELECT COALESCE(SUM(marks),0) FROM questions WHERE quiz_id=?", (quiz_id,)
    ).fetchone()[0]
    conn.close()

    percentage = (attempt["score"] / total_marks * 100) if total_marks else 0
    time_taken = None
    if attempt["submitted_at"]:
        time_taken = attempt["submitted_at"] - attempt["started_at"]

    return {
        "quiz_title": quiz["title"],
        "score": attempt["score"],
        "total_marks": total_marks,
        "percentage": round(percentage, 2),
        "correct": attempt["correct_count"],
        "wrong": attempt["wrong_count"],
        "time_taken": time_taken,
        "status": attempt["status"],
    }


# ---------------- teacher: live stats + reports + csv ----------------

@app.get("/api/quizzes/{quiz_id}/stats")
def quiz_stats(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    stats = get_stats(quiz_id)

    conn = get_db()
    avg = conn.execute(
        "SELECT AVG(score) FROM attempts WHERE quiz_id=? AND status='submitted'", (quiz_id,)
    ).fetchone()[0]
    top = conn.execute(
        "SELECT MAX(score) FROM attempts WHERE quiz_id=? AND status='submitted'", (quiz_id,)
    ).fetchone()[0]
    conn.close()

    return {
        **stats,
        "average_score": round(avg or 0, 2),
        "highest_score": round(top or 0, 2),
        "start_time": quiz["start_time"],
        "end_time": quiz["end_time"],
        "status": quiz["status"],
        "server_time": now(),
    }


def build_report_rows(quiz_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT u.name, u.username, a.score, a.correct_count, a.wrong_count, a.started_at, a.submitted_at
        FROM attempts a JOIN users u ON u.id = a.student_id
        WHERE a.quiz_id=?
        ORDER BY a.score DESC, a.submitted_at ASC
    """, (quiz_id,)).fetchall()
    conn.close()

    total_marks = sum(q["marks"] for q in get_questions(quiz_id))

    report = []
    for rank, r in enumerate(rows, start=1):
        pct = (r["score"] / total_marks * 100) if total_marks else 0
        time_taken = (r["submitted_at"] - r["started_at"]) if r["submitted_at"] else None
        report.append({
            "rank": rank,
            "student_name": r["name"],
            "student_id": r["username"],
            "score": r["score"],
            "percentage": round(pct, 2),
            "correct": r["correct_count"],
            "wrong": r["wrong_count"],
            "time_taken": time_taken,
        })
    return report


@app.get("/api/quizzes/{quiz_id}/students")
def students_report(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")
    return build_report_rows(quiz_id)


@app.get("/api/quizzes/{quiz_id}/export")
def export_csv(quiz_id: int, teacher=Depends(teacher_only)):
    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != teacher["id"]:
        raise HTTPException(status_code=403, detail="not your quiz")

    rows = build_report_rows(quiz_id)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Rank", "Name", "Student ID", "Score", "Percentage", "Correct", "Wrong", "Time Taken"])
    for r in rows:
        writer.writerow([r["rank"], r["student_name"], r["student_id"], r["score"],
                          r["percentage"], r["correct"], r["wrong"], r["time_taken"] or ""])
    output.seek(0)

    filename = "".join(c if c.isalnum() else "_" for c in quiz["title"]) + "_results.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------- platform info (just kept simple as a dict, no need for classes) ----------------

PLATFORMS = {
    "web": {"embedded_ui": False, "identity": True},
    "zoom": {"embedded_ui": True, "identity": "depends on zoom apps sdk"},
    "teams": {"embedded_ui": True, "identity": "microsoft identity"},
    "meet": {"embedded_ui": "depends", "identity": "depends"},
}


@app.get("/api/platforms")
def platforms():
    return PLATFORMS


# ---------------- websocket for live teacher monitoring ----------------

@app.websocket("/ws/quizzes/{quiz_id}")
async def quiz_ws(websocket: WebSocket, quiz_id: int):
    session_id = websocket.cookies.get("classquiz_session")
    user = SESSIONS.get(session_id)

    if not user or user["role"] != "teacher":
        await websocket.close(code=1008)
        return

    quiz = get_quiz_or_404(quiz_id)
    if quiz["teacher_id"] != user["id"]:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    LIVE_CLIENTS.setdefault(quiz_id, []).append(websocket)

    try:
        await websocket.send_json({"type": "stats", **get_stats(quiz_id)})

        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=25)
                if msg == "ping":
                    await websocket.send_json({"type": "pong", "server_time": now()})
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat", "server_time": now()})

    except WebSocketDisconnect:
        pass
    except:
        pass
    finally:
        if websocket in LIVE_CLIENTS.get(quiz_id, []):
            LIVE_CLIENTS[quiz_id].remove(websocket)


# ---------------- background task that checks for timed out quizzes/attempts ----------------

async def timeout_checker():
    while True:
        try:
            current = now()
            conn = get_db()

            expired_attempts = conn.execute("""
                SELECT a.id, a.quiz_id FROM attempts a
                JOIN quizzes q ON q.id = a.quiz_id
                WHERE a.status='active' AND q.status='live' AND q.end_time <= ?
            """, (current,)).fetchall()

            quiz_ids_touched = set()
            for a in expired_attempts:
                conn.execute(
                    "UPDATE attempts SET status='submitted', submitted_at=? WHERE id=? AND status='active'",
                    (current, a["id"]),
                )
                quiz_ids_touched.add(a["quiz_id"])
            conn.commit()

            for a in expired_attempts:
                grade_attempt(a["id"])

            for qid in quiz_ids_touched:
                clients = LIVE_CLIENTS.get(qid, [])
                for ws in clients[:]:
                    try:
                        await ws.send_json({"type": "attempt_timeout"})
                    except:
                        pass
                await broadcast_stats(qid)

            expired_quizzes = conn.execute(
                "SELECT id FROM quizzes WHERE status='live' AND end_time <= ?", (current,)
            ).fetchall()

            for q in expired_quizzes:
                conn.execute("UPDATE quizzes SET status='stopped' WHERE id=? AND status='live'", (q["id"],))
            conn.commit()
            conn.close()

            for q in expired_quizzes:
                clients = LIVE_CLIENTS.get(q["id"], [])
                for ws in clients[:]:
                    try:
                        await ws.send_json({"type": "quiz_stopped", "reason": "timer_expired"})
                    except:
                        pass

        except Exception as e:
            print("timeout checker error:", e)

        await asyncio.sleep(1)


# ---------------- frontend (same UI, just serving the html directly) ----------------

HTML_PAGE = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ClassQuiz</title>
<style>
* { box-sizing: border-box; }
body { margin:0; font-family: Arial, sans-serif; background:#f5f7fb; color:#111; }
header { background:#111827; color:#fff; padding:16px 24px; display:flex; justify-content:space-between; align-items:center; }
header h1 { margin:0; font-size:22px; }
main { max-width:1100px; margin:30px auto; padding:0 20px; }
.card { background:#fff; border-radius:12px; padding:20px; margin-bottom:20px; box-shadow:0 4px 14px rgba(0,0,0,0.06); }
input, textarea, select { width:100%; padding:10px; margin-top:6px; margin-bottom:14px; border:1px solid #ccc; border-radius:6px; }
button { border:0; border-radius:6px; padding:10px 15px; cursor:pointer; background:#2563eb; color:#fff; font-weight:600; margin-right:8px; }
button.secondary { background:#6b7280; }
button.danger { background:#dc2626; }
button.success { background:#16a34a; }
.hidden { display:none; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:15px; }
.stat { background:#f9fafb; border:1px solid #e5e7eb; padding:15px; border-radius:8px; }
.stat strong { display:block; font-size:26px; margin-top:5px; }
.question { border:1px solid #e5e7eb; border-radius:8px; padding:14px; margin:10px 0; }
.option { display:block; margin:8px 0; padding:9px; background:#f9fafb; border-radius:6px; }
.timer { font-size:30px; font-weight:800; text-align:center; padding:12px; }
.error { color:#dc2626; margin:8px 0; }
table { width:100%; border-collapse:collapse; }
th, td { padding:8px; border-bottom:1px solid #e5e7eb; text-align:left; }
</style>
</head>
<body>

<header>
  <h1>ClassQuiz</h1>
  <div>
    <span id="userInfo"></span>
    <button id="logoutBtn" class="secondary hidden" onclick="logout()">Logout</button>
  </div>
</header>

<main>

  <section id="loginSection" class="card">
    <h2>Login</h2>
    <p>Demo Teacher: <b>teacher / teacher123</b></p>
    <p>Demo Student: <b>student001 / student123</b></p>
    <input id="username" placeholder="Username">
    <input id="password" type="password" placeholder="Password">
    <button onclick="login()">Login</button>
    <div id="loginError" class="error"></div>
  </section>

  <section id="teacherSection" class="hidden">
    <div class="card">
      <h2>Create Quiz</h2>
      <input id="quizTitle" placeholder="Quiz title">
      <textarea id="quizDescription" placeholder="Description"></textarea>
      <label>Duration (seconds)</label>
      <input id="quizDuration" type="number" value="60" min="10">
      <label>Negative marking per wrong answer</label>
      <input id="negativeMarking" type="number" value="0" min="0" step="0.25">
      <button onclick="createQuiz()">Create Quiz</button>
    </div>

    <div id="questionBuilder" class="card hidden">
      <h2>Add Question</h2>
      <p>Quiz ID: <b id="currentQuizId"></b></p>
      <textarea id="questionText" placeholder="Question"></textarea>
      <input id="option0" placeholder="Option A">
      <input id="option1" placeholder="Option B">
      <input id="option2" placeholder="Option C">
      <input id="option3" placeholder="Option D">
      <label>Correct option</label>
      <select id="correctIndex">
        <option value="0">A</option>
        <option value="1">B</option>
        <option value="2">C</option>
        <option value="3">D</option>
      </select>
      <label>Marks</label>
      <input id="questionMarks" type="number" value="1" min="0.1" step="0.1">
      <button onclick="addQuestion()">Add Question</button>
      <button class="success" onclick="startCurrentQuiz()">Start Quiz</button>
    </div>

    <div class="card">
      <h2>Your Quizzes</h2>
      <button onclick="loadTeacherQuizzes()">Refresh</button>
      <div id="teacherQuizzes"></div>
    </div>

    <div id="teacherLive" class="card hidden">
      <h2>Live Monitoring</h2>
      <div class="timer"><span id="teacherTimer">--</span></div>
      <div class="grid">
        <div class="stat">Participants<strong id="participants">0</strong></div>
        <div class="stat">Submitted<strong id="submitted">0</strong></div>
        <div class="stat">Not Submitted<strong id="notSubmitted">0</strong></div>
      </div>
      <br>
      <button class="danger" onclick="stopCurrentQuiz()">Stop Quiz</button>
      <button onclick="exportResults()">Export CSV</button>
      <div id="studentReports"></div>
    </div>
  </section>

  <section id="studentSection" class="hidden">
    <div class="card">
      <h2>Join Live Quiz</h2>
      <input id="joinQuizId" type="number" placeholder="Quiz ID">
      <button onclick="joinQuiz()">Join Quiz</button>
      <div id="studentError" class="error"></div>
    </div>

    <div id="studentQuiz" class="hidden">
      <div class="card">
        <h2 id="studentQuizTitle"></h2>
        <div id="studentTimer" class="timer">--</div>
        <div id="questions"></div>
        <button class="success" onclick="submitQuiz()">Submit Quiz</button>
      </div>
    </div>

    <div id="studentResult" class="card hidden">
      <h2>Result</h2>
      <div id="resultContent"></div>
    </div>
  </section>

</main>

<script>
let currentUser = null;
let currentQuizId = null;
let teacherSocket = null;
let serverOffset = 0;
let timerInterval = null;

async function api(url, options={}) {
  const res = await fetch(url, {
    credentials: "include",
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers||{}) }
  });
  const data = await res.json().catch(()=>({}));
  if (!res.ok) throw new Error(data.detail || "request failed");
  return data;
}

async function login() {
  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;
  try {
    const data = await api("/api/login", { method:"POST", body: JSON.stringify({username, password}) });
    document.cookie = "classquiz_session=" + data.session_id + "; path=/";
    currentUser = data.user;
    afterLogin();
  } catch(e) {
    document.getElementById("loginError").textContent = e.message;
  }
}

function afterLogin() {
  document.getElementById("loginSection").classList.add("hidden");
  document.getElementById("logoutBtn").classList.remove("hidden");
  document.getElementById("userInfo").textContent = currentUser.name + " (" + currentUser.role + ")";
  if (currentUser.role === "teacher") {
    document.getElementById("teacherSection").classList.remove("hidden");
    loadTeacherQuizzes();
  } else {
    document.getElementById("studentSection").classList.remove("hidden");
  }
}

async function logout() {
  try { await api("/api/logout", {method:"POST"}); } catch(e) {}
  document.cookie = "classquiz_session=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT";
  location.reload();
}

async function createQuiz() {
  try {
    const data = await api("/api/quizzes", { method:"POST", body: JSON.stringify({
      title: document.getElementById("quizTitle").value,
      description: document.getElementById("quizDescription").value,
      duration_seconds: Number(document.getElementById("quizDuration").value),
      negative_marking: Number(document.getElementById("negativeMarking").value)
    })});
    currentQuizId = data.quiz_id;
    document.getElementById("currentQuizId").textContent = currentQuizId;
    document.getElementById("questionBuilder").classList.remove("hidden");
    alert("Quiz created. ID = " + currentQuizId);
    loadTeacherQuizzes();
  } catch(e) { alert(e.message); }
}

async function addQuestion() {
  if (!currentQuizId) { alert("create a quiz first"); return; }
  const options = [
    document.getElementById("option0").value,
    document.getElementById("option1").value,
    document.getElementById("option2").value,
    document.getElementById("option3").value
  ].filter(v => v.trim());
  try {
    await api("/api/quizzes/" + currentQuizId + "/questions", { method:"POST", body: JSON.stringify({
      question_text: document.getElementById("questionText").value,
      options,
      correct_index: Number(document.getElementById("correctIndex").value),
      marks: Number(document.getElementById("questionMarks").value)
    })});
    document.getElementById("questionText").value = "";
    alert("question added");
  } catch(e) { alert(e.message); }
}

async function loadTeacherQuizzes() {
  try {
    const quizzes = await api("/api/teacher/quizzes");
    const box = document.getElementById("teacherQuizzes");
    box.innerHTML = "";
    quizzes.forEach(q => {
      const div = document.createElement("div");
      div.className = "question";
      div.innerHTML = `<b>${escapeHtml(q.title)}</b>
        <p>ID: ${q.id}<br>Status: ${q.status}<br>Duration: ${q.duration_seconds}s</p>
        <button onclick="selectQuiz(${q.id})">Manage</button>`;
      box.appendChild(div);
    });
  } catch(e) { console.error(e); }
}

async function selectQuiz(id) {
  currentQuizId = id;
  document.getElementById("currentQuizId").textContent = id;
  document.getElementById("questionBuilder").classList.remove("hidden");
  try {
    const data = await api("/api/quizzes/" + id);
    if (data.quiz.status === "live") openTeacherLive();
  } catch(e) { alert(e.message); }
}

async function startCurrentQuiz() {
  if (!currentQuizId) { alert("select a quiz first"); return; }
  try {
    await api("/api/quizzes/" + currentQuizId + "/start", {method:"POST"});
    openTeacherLive();
  } catch(e) { alert(e.message); }
}

function openTeacherLive() {
  document.getElementById("teacherLive").classList.remove("hidden");
  connectTeacherSocket();
  loadStats();
  loadReports();
}

function connectTeacherSocket() {
  if (teacherSocket) teacherSocket.close();
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  teacherSocket = new WebSocket(proto + "//" + location.host + "/ws/quizzes/" + currentQuizId);
  teacherSocket.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "stats") updateStats(msg);
    if (msg.type === "quiz_stopped") { alert("quiz stopped"); loadReports(); }
  };
  teacherSocket.onclose = () => {
    setTimeout(() => { if (currentQuizId) connectTeacherSocket(); }, 2000);
  };
}

function updateStats(stats) {
  document.getElementById("participants").textContent = stats.participants ?? 0;
  document.getElementById("submitted").textContent = stats.submitted ?? 0;
  document.getElementById("notSubmitted").textContent = stats.not_submitted ?? 0;
  if (stats.end_time) startTimer("teacherTimer", stats.end_time);
}

async function loadStats() {
  if (!currentQuizId) return;
  try { updateStats(await api("/api/quizzes/" + currentQuizId + "/stats")); } catch(e) {}
}

async function stopCurrentQuiz() {
  if (!currentQuizId || !confirm("stop this quiz?")) return;
  try {
    await api("/api/quizzes/" + currentQuizId + "/stop", {method:"POST"});
    loadStats(); loadReports();
  } catch(e) { alert(e.message); }
}

async function loadReports() {
  if (!currentQuizId) return;
  try {
    const reports = await api("/api/quizzes/" + currentQuizId + "/students");
    let html = "<h3>Results</h3>";
    if (reports.length === 0) { html += "<p>No students yet.</p>"; }
    else {
      html += `<table><thead><tr><th>Rank</th><th>Student</th><th>ID</th><th>Score</th><th>%</th><th>Correct</th><th>Wrong</th><th>Time</th></tr></thead><tbody>`;
      reports.forEach(r => {
        html += `<tr><td>${r.rank}</td><td>${escapeHtml(r.student_name)}</td><td>${escapeHtml(r.student_id)}</td>
          <td>${r.score}</td><td>${r.percentage}</td><td>${r.correct}</td><td>${r.wrong}</td><td>${r.time_taken ?? "-"}</td></tr>`;
      });
      html += "</tbody></table>";
    }
    document.getElementById("studentReports").innerHTML = html;
  } catch(e) { console.error(e); }
}

function exportResults() {
  if (!currentQuizId) return;
  window.location.href = "/api/quizzes/" + currentQuizId + "/export";
}

async function joinQuiz() {
  const quizId = Number(document.getElementById("joinQuizId").value);
  if (!quizId) return;
  try {
    const data = await api("/api/quizzes/" + quizId + "/join", {method:"POST"});
    currentQuizId = quizId;
    serverOffset = data.server_time - (Date.now()/1000);
    document.getElementById("studentQuizTitle").textContent = data.quiz.title;
    const box = document.getElementById("questions");
    box.innerHTML = "";
    data.questions.forEach(q => {
      const div = document.createElement("div");
      div.className = "question";
      let optsHtml = "";
      q.options.forEach(o => {
        optsHtml += `<label class="option"><input type="radio" name="q_${q.id}" value="${o.id}"
          onchange="saveAnswer(${q.id}, ${o.id})"> ${escapeHtml(o.text)}</label>`;
      });
      div.innerHTML = `<b>${q.position+1}. ${escapeHtml(q.question_text)}</b>${optsHtml}`;
      box.appendChild(div);
    });
    document.getElementById("studentQuiz").classList.remove("hidden");
    startTimer("studentTimer", data.quiz.end_time);
  } catch(e) {
    document.getElementById("studentError").textContent = e.message;
  }
}

async function saveAnswer(questionId, optionId) {
  try {
    await api("/api/quizzes/" + currentQuizId + "/answer", { method:"POST", body: JSON.stringify({
      question_id: questionId, option_id: optionId
    })});
  } catch(e) { alert("answer not saved: " + e.message); }
}

async function submitQuiz() {
  if (!currentQuizId) return;
  try {
    const result = await api("/api/quizzes/" + currentQuizId + "/submit", {method:"POST"});
    showResult(result);
  } catch(e) { alert(e.message); }
}

function showResult(result) {
  document.getElementById("studentQuiz").classList.add("hidden");
  document.getElementById("studentResult").classList.remove("hidden");
  document.getElementById("resultContent").innerHTML = `
    <p><b>Score:</b> ${result.score}</p>
    <p><b>Correct:</b> ${result.correct}</p>
    <p><b>Wrong:</b> ${result.wrong}</p>`;
}

function startTimer(elementId, endTime) {
  if (timerInterval) clearInterval(timerInterval);
  function update() {
    const cur = Date.now()/1000 + serverOffset;
    const remaining = Math.max(0, Math.floor(endTime - cur));
    const m = Math.floor(remaining/60), s = remaining%60;
    document.getElementById(elementId).textContent = String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
    if (remaining <= 0) {
      clearInterval(timerInterval);
      if (currentUser && currentUser.role === "student") submitQuiz();
    }
  }
  update();
  timerInterval = setInterval(update, 1000);
}

function escapeHtml(v) {
  const d = document.createElement("div");
  d.textContent = String(v);
  return d.innerHTML;
}

async function restoreSession() {
  try {
    const user = await api("/api/me");
    currentUser = user;
    afterLogin();
  } catch(e) {}
}

restoreSession();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(content=HTML_PAGE)


@app.on_event("startup")
async def startup():
    setup_db()
    seed_users()
    asyncio.create_task(timeout_checker())
    print("ClassQuiz running at http://" + HOST + ":" + str(PORT))
    print("teacher login -> teacher / teacher123")
    print("student login -> student001 / student123 (up to student050)")


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)