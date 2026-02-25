from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import db
import json
import traceback
import time
import random
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = "sliding_puzzle_secret_key"
app.config['SESSION_TIMEOUT_MINUTES'] = 30  # Таймаут сессии в минутах

# ================================================================
# КОНФИГУРАЦИЯ ПЕРЕМЕШИВАНИЯ
# ================================================================

# Количество ходов для перемешивания в зависимости от сложности
SHUFFLE_MOVES = {
    'Easy': 50,
    'Medium': 200,
    'Hard': 500
}

# Множители для разных размеров поля
SIZE_MULTIPLIERS = {
    3: 1,  # 3x3 - базовый
    4: 2,  # 4x4 - в 2 раза больше ходов
    5: 3,  # 5x5 - в 3 раза больше ходов
    6: 4,  # 6x6 - в 4 раза больше ходов
    7: 5  # 7x7 - в 5 раз больше ходов
}


# ================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ================================================================

def get_current_user_id():
    return session.get("user_id")


def get_active_session_id():
    """Возвращает ID активной игровой сессии из Flask-сессии."""
    try:
        return session.get("game_session_id")
    except:
        return None


def ensure_db_connection():
    """Проверяет и восстанавливает соединение с БД."""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Простой запрос для проверки соединения
            result = db.fetch_one("SELECT 1 FROM DUAL")
            if result:
                return True
        except Exception as e:
            print(f"Database connection lost (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(0.5)
            else:
                return False
    return False


def cleanup_stale_sessions(timeout_minutes=None):
    """
    Завершает все активные сессии, которые были бездействия дольше timeout_minutes.
    Вызывается перед каждым запросом.
    """
    if timeout_minutes is None:
        timeout_minutes = app.config['SESSION_TIMEOUT_MINUTES']

    try:
        if not ensure_db_connection():
            return

        # Получаем статус 'abandoned' для завершения
        status_abandoned = db.fetch_one(
            "SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'"
        )
        if not status_abandoned:
            # Создаем, если не существует
            db.execute_query(
                "INSERT INTO GAME_STATUSES (ID, NAME) VALUES (SEQ_GAME_STATUSES.NEXTVAL, 'abandoned')"
            )
            status_abandoned = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'")

        # Находим все активные сессии, у которых LAST_ACTIVITY_AT старше timeout_minutes
        stale_sessions = db.fetch_all(
            f"""SELECT GS.ID, GS.USER_ID, GA.ID as ATTEMPT_ID
                FROM GAME_SESSIONS GS
                JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
                JOIN GAME_ATTEMPTS GA ON GS.ID = GA.SESSION_ID 
                    AND GA.STATUS_ID = GST.ID
                WHERE GST.NAME = 'active'
                AND GS.LAST_ACTIVITY_AT < SYSTIMESTAMP - INTERVAL '{timeout_minutes}' MINUTE"""
        )

        for session_data in stale_sessions:
            # Завершаем попытку
            db.execute_query(
                """UPDATE GAME_ATTEMPTS 
                   SET STATUS_ID = :1, FINISHED_AT = SYSTIMESTAMP 
                   WHERE ID = :2""",
                [status_abandoned["id"], session_data["attempt_id"]]
            )

            # Завершаем сессию
            db.execute_query(
                """UPDATE GAME_SESSIONS 
                   SET STATUS_ID = :1, END_TIME = SYSDATE 
                   WHERE ID = :2""",
                [status_abandoned["id"], session_data["id"]]
            )

            # Если это текущая сессия пользователя, очищаем её из сессии Flask
            current_session_id = get_active_session_id()
            if current_session_id == session_data["id"]:
                session.pop("game_session_id", None)

            # Логируем событие
            try:
                db.execute_query(
                    """INSERT INTO LOGS (ID, LOG_DATE, SESSION_ID, LOG_TYPE, PROCEDURE_NAME, MESSAGE)
                       VALUES (SEQ_LOGS.NEXTVAL, SYSDATE, :1, 'INFO', 'AUTO_CLEANUP', :2)""",
                    [session_data["id"], f"Session auto-closed after {timeout_minutes} minutes of inactivity"]
                )
            except:
                pass  # Игнорируем ошибки логирования

        if stale_sessions:
            print(f"Auto-closed {len(stale_sessions)} stale sessions")

    except Exception as e:
        print(f"Error in cleanup_stale_sessions: {e}")


def check_current_session_valid():
    """Проверяет, не истекла ли текущая активная сессия по времени"""
    gsid = get_active_session_id()
    if not gsid:
        return True  # Нет активной сессии - ок

    try:
        session_data = db.fetch_one(
            """SELECT GS.LAST_ACTIVITY_AT, GST.NAME as STATUS_NAME
               FROM GAME_SESSIONS GS
               JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
               WHERE GS.ID = :1""",
            [gsid]
        )

        if not session_data:
            # Сессия не найдена
            session.pop("game_session_id", None)
            return False

        if session_data["status_name"] != 'active':
            # Сессия уже не активна
            session.pop("game_session_id", None)
            return False

        # Проверяем время последней активности
        last_activity = session_data["last_activity_at"]
        if hasattr(last_activity, 'timestamp'):
            # Если это datetime объект
            timeout_minutes = app.config['SESSION_TIMEOUT_MINUTES']
            if datetime.now() - last_activity > timedelta(minutes=timeout_minutes):
                # Завершаем сессию
                status_abandoned = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'")
                if status_abandoned:
                    # Получаем ID попытки
                    attempt = db.fetch_one(
                        "SELECT ID FROM GAME_ATTEMPTS WHERE SESSION_ID = :1 AND STATUS_ID = (SELECT ID FROM GAME_STATUSES WHERE NAME='active')",
                        [gsid]
                    )
                    if attempt:
                        db.execute_query(
                            "UPDATE GAME_ATTEMPTS SET STATUS_ID = :1, FINISHED_AT = SYSTIMESTAMP WHERE ID = :2",
                            [status_abandoned["id"], attempt["id"]]
                        )

                    db.execute_query(
                        "UPDATE GAME_SESSIONS SET STATUS_ID = :1, END_TIME = SYSDATE WHERE ID = :2",
                        [status_abandoned["id"], gsid]
                    )

                session.pop("game_session_id", None)
                return False
    except Exception as e:
        print(f"Error in check_current_session_valid: {e}")
        return False

    return True


@app.before_request
def before_request():
    """Выполняется перед каждым запросом"""
    # Очищаем старые сессии (раз в час примерно)
    if not hasattr(app, 'last_cleanup') or time.time() - app.last_cleanup > 3600:
        cleanup_stale_sessions()
        app.last_cleanup = time.time()

    # Проверяем валидность текущей сессии
    if get_current_user_id() and get_active_session_id():
        if not check_current_session_valid():
            # Если сессия невалидна, редиректим на главную
            if request.endpoint not in ['login', 'static'] and not request.path.startswith('/static'):
                return redirect(url_for('index'))


def get_active_attempt(session_id):
    """Возвращает активную попытку по session_id."""
    if not session_id:
        return None

    if not ensure_db_connection():
        print("Cannot connect to database in get_active_attempt")
        return None

    try:
        return db.fetch_one(
            """SELECT GA.ID, GA.CURRENT_STATE, GA.UNDO_POINTER,
                      GA.CURRENT_MISPLACED_TILES, GA.CURRENT_MANHATTAN_DISTANCE,
                      GA.INITIAL_MANHATTAN_DISTANCE,
                      PS.GRID_SIZE, DL.NAME AS DIFFICULTY,
                      PZ.TARGET_STATE, PZ.ID AS PUZZLE_ID,
                      GA.STARTED_AT
               FROM GAME_ATTEMPTS GA
               JOIN GAME_STATUSES GST ON GA.STATUS_ID = GST.ID
               JOIN PUZZLES PZ ON GA.PUZZLE_ID = PZ.ID
               JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
               JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
               WHERE GA.SESSION_ID = :1 AND GST.NAME = 'active' AND ROWNUM = 1""",
            [session_id]
        )
    except Exception as e:
        print(f"Error in get_active_attempt: {e}")
        return None


def read_clob(value):
    """Читает CLOB или строку и возвращает str. При ошибке возвращает пустую строку."""
    if value is None:
        return ""

    # Если это уже строка
    if isinstance(value, str):
        return value

    # Если это CLOB объект
    if hasattr(value, "read"):
        try:
            # Пробуем прочитать CLOB
            result = value.read()
            # Пробуем закрыть CLOB
            try:
                value.close()
            except:
                pass
            # Если результат - байты, декодируем
            if isinstance(result, bytes):
                result = result.decode('utf-8')
            return result if result else ""
        except Exception as e:
            print(f"Error reading CLOB: {e}")
            # Пробуем альтернативный метод
            try:
                if hasattr(value, 'read'):
                    value = str(value)
                    return value
            except:
                pass
            return ""

    # В остальных случаях просто преобразуем в строку
    try:
        return str(value)
    except:
        return ""


def parse_board(state_str, grid_size):
    """Преобразует состояние в двумерный список и flat-список."""
    s = read_clob(state_str)
    s = s.strip()

    if not s:
        return [], []

    # Убираем лишние кавычки если они есть
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]

    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        # Если не JSON, пробуем CSV
        return parse_board_csv(s, grid_size)

    # Проверяем формат данных
    if data and len(data) > 0 and isinstance(data[0], list):
        # Это двумерный массив
        flat = []
        for row in data:
            flat.extend(row)
        return data, flat
    else:
        # Это плоский список
        flat = data
        expected_length = grid_size * grid_size
        if len(flat) != expected_length:
            if len(flat) > expected_length:
                flat = flat[:expected_length]
            elif len(flat) < expected_length:
                flat.extend([0] * (expected_length - len(flat)))

        board = []
        for r in range(grid_size):
            start_idx = r * grid_size
            end_idx = (r + 1) * grid_size
            if end_idx <= len(flat):
                board.append(flat[start_idx:end_idx])
            else:
                board.append([])
        return board, flat


def parse_board_csv(csv_str, grid_size):
    """Парсит CSV-строку в доску и flat-список."""
    try:
        flat = [int(x.strip()) for x in csv_str.split(",")]
    except ValueError:
        return [], []

    expected_length = grid_size * grid_size
    if len(flat) != expected_length:
        if len(flat) > expected_length:
            flat = flat[:expected_length]
        elif len(flat) < expected_length:
            flat.extend([0] * (expected_length - len(flat)))

    board = []
    for r in range(grid_size):
        start_idx = r * grid_size
        end_idx = (r + 1) * grid_size
        if end_idx <= len(flat):
            board.append(flat[start_idx:end_idx])
        else:
            board.append([])
    return board, flat


def flat_to_json(flat):
    """Преобразует плоский список в JSON-строку."""
    return json.dumps(flat)


def compute_metrics(flat, target_flat, grid_size):
    """Считает misplaced и manhattan distance."""
    misplaced = 0
    manhattan = 0
    correct = 0
    n = grid_size

    # Убеждаемся, что target_flat - плоский список
    if target_flat and len(target_flat) > 0 and isinstance(target_flat[0], list):
        new_target = []
        for row in target_flat:
            new_target.extend(row)
        target_flat = new_target

    target_positions = {}
    for idx, val in enumerate(target_flat):
        if val != 0:
            target_positions[val] = idx

    for i, val in enumerate(flat):
        if val == 0:
            continue

        if val in target_positions:
            target_pos = target_positions[val]

            if i == target_pos:
                correct += 1
            else:
                misplaced += 1

            cur_row, cur_col = divmod(i, n)
            tgt_row, tgt_col = divmod(target_pos, n)
            manhattan += abs(cur_row - tgt_row) + abs(cur_col - tgt_col)
        else:
            misplaced += 1
            manhattan += 2 * n

    return misplaced, manhattan, correct


def progress_pct(init_manhattan, cur_manhattan):
    if init_manhattan == 0:
        return 100
    return round((init_manhattan - cur_manhattan) / init_manhattan * 100, 1)


def check_win_condition(flat, target_flat):
    """Проверяет, решена ли головоломка."""
    if len(flat) != len(target_flat):
        return False

    for i in range(len(flat)):
        if flat[i] != target_flat[i]:
            return False
    return True


# ================================================================
# ФУНКЦИИ ПЕРЕМЕШИВАНИЯ
# ================================================================

def shuffle_board(flat_board, grid_size, moves_count):
    """
    Перемешивает доску, выполняя случайные допустимые ходы.

    Args:
        flat_board: плоский список с состоянием доски
        grid_size: размер сетки
        moves_count: количество ходов для перемешивания

    Returns:
        новое перемешанное состояние (плоский список)
    """
    # Создаем копию доски для перемешивания
    board = flat_board.copy()
    n = grid_size

    for _ in range(moves_count):
        # Находим позицию пустой клетки (0)
        try:
            empty_pos = board.index(0)
        except ValueError:
            # Если нет пустой клетки, создаем её в конце
            board[-1] = 0
            empty_pos = n * n - 1

        # Находим возможные ходы (соседние клетки)
        possible_moves = []

        # Вверх
        if empty_pos >= n:
            possible_moves.append(empty_pos - n)
        # Вниз
        if empty_pos < n * n - n:
            possible_moves.append(empty_pos + n)
        # Влево
        if empty_pos % n != 0:
            possible_moves.append(empty_pos - 1)
        # Вправо
        if empty_pos % n != n - 1:
            possible_moves.append(empty_pos + 1)

        if possible_moves:
            # Выбираем случайный ход
            move_pos = random.choice(possible_moves)
            # Меняем местами пустую клетку с выбранной
            board[empty_pos], board[move_pos] = board[move_pos], board[empty_pos]

    return board


def shuffle_board_with_seed(flat_board, grid_size, difficulty, seed=None):
    """
    Перемешивает доску с использованием seed для воспроизводимости.

    Args:
        flat_board: плоский список с состоянием доски
        grid_size: размер сетки
        difficulty: уровень сложности ('Easy', 'Medium', 'Hard')
        seed: опциональный seed для random

    Returns:
        перемешанное состояние и количество сделанных ходов
    """
    # Определяем количество ходов в зависимости от сложности
    moves_count = SHUFFLE_MOVES.get(difficulty, 50)

    # Учитываем размер поля: для больших полей нужно больше ходов
    multiplier = SIZE_MULTIPLIERS.get(grid_size, 1)
    moves_count = moves_count * multiplier

    # Устанавливаем seed для воспроизводимости
    if seed:
        random.seed(seed)

    # Перемешиваем
    shuffled = shuffle_board(flat_board, grid_size, moves_count)

    # Сбрасываем seed
    if seed:
        random.seed()

    return shuffled, moves_count


def verify_shuffled(flat_board, target_flat):
    """Проверяет, что поле не совпадает с целевым состоянием"""
    return flat_board != target_flat


def count_misplaced_tiles(flat_board, target_flat):
    """Считает количество неправильно расположенных плиток"""
    return sum(1 for i, val in enumerate(flat_board)
               if val != 0 and val != target_flat[i])


# ================================================================
# АВТОРИЗАЦИЯ
# ================================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        if not username:
            error = "Введите имя пользователя."
        else:
            user = db.fetch_one(
                "SELECT ID, USERNAME FROM USERS WHERE USERNAME = :1",
                [username]
            )
            if not user:
                db.execute_query(
                    "INSERT INTO USERS (ID, DB_USERNAME, USERNAME, GAMES_COUNT, CREATED_AT) "
                    "VALUES (SEQ_USERS.NEXTVAL, :1, :2, 0, SYSTIMESTAMP)",
                    [username, username]
                )
                user = db.fetch_one(
                    "SELECT ID, USERNAME FROM USERS WHERE USERNAME = :1",
                    [username]
                )
            session["user_id"] = user["id"]
            session["username"] = user["username"]

            # Восстановить активную сессию если есть и она не устарела (менее 30 минут назад)
            active = db.fetch_one(
                """SELECT GS.ID 
                   FROM GAME_SESSIONS GS
                   JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
                   WHERE GS.USER_ID = :1 
                   AND GST.NAME = 'active' 
                   AND GS.LAST_ACTIVITY_AT > SYSTIMESTAMP - INTERVAL '30' MINUTE
                   AND ROWNUM = 1""",
                [user["id"]]
            )
            if active:
                session["game_session_id"] = active["id"]
            return redirect(url_for("index"))
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ================================================================
# ГЛАВНАЯ
# ================================================================

@app.route("/")
def index():
    if not get_current_user_id():
        return redirect(url_for("login"))

    puzzles = db.fetch_all(
        """SELECT P.ID, P.SEED, PS.GRID_SIZE, DL.NAME AS DIFFICULTY,
                  DL.SHUFFLE_MOVES,
                  P.IS_DAILY,
                  COUNT(DISTINCT GS.ID) AS TIMES_PLAYED,
                  SUM(CASE WHEN GST.NAME='solved' THEN 1 ELSE 0 END) AS TIMES_SOLVED
           FROM PUZZLES P
           JOIN PUZZLE_SIZES PS ON P.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON P.DIFFICULTY_ID = DL.ID
           LEFT JOIN GAME_SESSIONS GS ON P.ID = GS.PUZZLE_ID
           LEFT JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
           GROUP BY P.ID, P.SEED, PS.GRID_SIZE, DL.NAME, DL.SHUFFLE_MOVES, P.IS_DAILY, DL.ID
           ORDER BY DL.ID, PS.GRID_SIZE"""
    )

    daily = db.fetch_one(
        """SELECT P.ID AS PUZZLE_ID, P.SEED, PS.GRID_SIZE, DL.NAME AS DIFFICULTY
           FROM PUZZLES P
           JOIN PUZZLE_SIZES PS ON P.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON P.DIFFICULTY_ID = DL.ID
           WHERE P.IS_DAILY = 1 AND ROWNUM = 1"""
    )

    active_game = None
    gsid = get_active_session_id()
    if gsid:
        # Проверяем, что сессия действительно активна
        active_check = db.fetch_one(
            """SELECT GS.ID 
               FROM GAME_SESSIONS GS
               JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
               WHERE GS.ID = :1 AND GST.NAME = 'active'""",
            [gsid]
        )
        if active_check:
            active_game = get_active_attempt(gsid)
        else:
            session.pop("game_session_id", None)

    return render_template(
        "index.html",
        puzzles=puzzles,
        daily=daily,
        active_game=active_game,
        username=session.get("username")
    )


# ================================================================
# ИГРА -- ЗАПУСК
# ================================================================

@app.route("/game/start/<int:puzzle_id>")
def start_game(puzzle_id):
    if not get_current_user_id():
        return redirect(url_for("login"))

    gsid = get_active_session_id()
    if gsid:
        # Проверяем, активна ли существующая сессия
        active_check = db.fetch_one(
            """SELECT GS.ID 
               FROM GAME_SESSIONS GS
               JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
               WHERE GS.ID = :1 AND GST.NAME = 'active'""",
            [gsid]
        )
        if active_check:
            return redirect(url_for("game"))
        else:
            session.pop("game_session_id", None)

    user_id = get_current_user_id()

    # Получаем информацию о пазле, включая уровень сложности
    puzzle = db.fetch_one(
        """SELECT PZ.ID, PS.GRID_SIZE, DL.NAME AS DIFFICULTY_NAME, DL.SHUFFLE_MOVES,
                  PZ.INITIAL_STATE, PZ.TARGET_STATE, PS.DEFAULT_TIME_LIMIT,
                  PZ.SEED
           FROM PUZZLES PZ
           JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
           WHERE PZ.ID = :1""",
        [puzzle_id]
    )
    if not puzzle:
        return redirect(url_for("index"))

    grid_size = puzzle["grid_size"]
    difficulty_name = puzzle["difficulty_name"]
    puzzle_seed = puzzle["seed"]

    # Читаем целевое состояние (решенное)
    target_json = read_clob(puzzle["target_state"])

    try:
        target_data = json.loads(target_json)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return redirect(url_for("index"))

    # Преобразуем целевое состояние в плоский список
    if target_data and len(target_data) > 0 and isinstance(target_data[0], list):
        target_flat = []
        for row in target_data:
            target_flat.extend(row)
    else:
        target_flat = target_data

    # СОЗДАЕМ НАЧАЛЬНОЕ СОСТОЯНИЕ ПУТЕМ ПЕРЕМЕШИВАНИЯ ЦЕЛЕВОГО
    # Используем seed пазла + timestamp для уникальности каждой игры
    game_seed = f"{puzzle_seed}_{int(time.time())}"

    shuffled_flat, moves_done = shuffle_board_with_seed(
        target_flat,
        grid_size,
        difficulty_name,
        seed=game_seed
    )

    # Проверяем, что поле действительно перемешано
    if not verify_shuffled(shuffled_flat, target_flat):
        # Если не перемешалось (маловероятно), делаем еще одну попытку
        shuffled_flat, moves_done = shuffle_board_with_seed(
            target_flat,
            grid_size,
            difficulty_name,
            seed=f"{game_seed}_2"
        )

    # Преобразуем перемешанное состояние в JSON
    shuffled_json = json.dumps(shuffled_flat)

    # Вычисляем метрики для перемешанного состояния
    misplaced, manhattan, correct = compute_metrics(shuffled_flat, target_flat, grid_size)

    # Получаем статус 'active'
    status_active_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='active'")
    if not status_active_row:
        db.execute_query("INSERT INTO GAME_STATUSES (ID, NAME) VALUES (SEQ_GAME_STATUSES.NEXTVAL, 'active')")
        status_active_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='active'")
    status_active = status_active_row["id"]

    # Получаем тип действия 'move'
    action_move_row = db.fetch_one("SELECT ID FROM ACTION_TYPES WHERE NAME='move'")
    if not action_move_row:
        db.execute_query("INSERT INTO ACTION_TYPES (ID, NAME) VALUES (SEQ_ACTION_TYPES.NEXTVAL, 'move')")
        action_move_row = db.fetch_one("SELECT ID FROM ACTION_TYPES WHERE NAME='move'")
    action_move = action_move_row["id"]

    # Создаем токен сессии
    timestamp_row = db.fetch_one("SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') AS T FROM DUAL")
    token = f"{user_id}_{puzzle_id}_{timestamp_row['t']}"

    # Создаем игровую сессию
    db.execute_query(
        """INSERT INTO GAME_SESSIONS
               (ID, USER_ID, PUZZLE_ID, STATUS_ID, SESSION_TOKEN,
                STEPS_COUNT, LAST_ACTIVITY_AT, START_TIME)
           VALUES (SEQ_GAME_SESSIONS.NEXTVAL, :1, :2, :3, :4,
                   0, SYSTIMESTAMP, SYSDATE)""",
        [user_id, puzzle_id, status_active, token]
    )

    # Получаем ID сессии
    gs = db.fetch_one("SELECT ID FROM GAME_SESSIONS WHERE SESSION_TOKEN=:1", [token])
    if not gs:
        gs = db.fetch_one(
            "SELECT ID FROM GAME_SESSIONS WHERE USER_ID = :1 AND SESSION_TOKEN LIKE :2 ORDER BY ID DESC",
            [user_id, f"{user_id}_{puzzle_id}%"]
        )
    gs_id = gs["id"]

    # Создаем игровую попытку с ПЕРЕМЕШАННЫМ состоянием
    db.execute_query(
        """INSERT INTO GAME_ATTEMPTS
               (ID, SESSION_ID, USER_ID, PUZZLE_ID, GAME_MODE, CURRENT_STATE,
                INITIAL_MISPLACED_TILES, INITIAL_MANHATTAN_DISTANCE,
                CURRENT_MISPLACED_TILES, CURRENT_MANHATTAN_DISTANCE,
                UNDO_POINTER, STATUS_ID, STARTED_AT)
           VALUES (SEQ_GAME_ATTEMPTS.NEXTVAL, :1, :2, :3, 'numbers', :4,
                   :5, :6, :7, :8, 0, :9, SYSTIMESTAMP)""",
        [gs_id, user_id, puzzle_id, shuffled_json,
         misplaced, manhattan, misplaced, manhattan, status_active]
    )

    # Получаем ID попытки
    ga = db.fetch_one("SELECT ID FROM GAME_ATTEMPTS WHERE SESSION_ID=:1 AND ROWNUM=1", [gs_id])
    if not ga:
        ga = db.fetch_one(
            "SELECT ID FROM GAME_ATTEMPTS WHERE SESSION_ID = :1 ORDER BY ID DESC",
            [gs_id]
        )
    ga_id = ga["id"]

    # Проверяем, существует ли уже шаг с индексом 0
    existing_step = db.fetch_one(
        "SELECT ID FROM GAME_STEPS WHERE ATTEMPT_ID = :1 AND STEP_INDEX = 0",
        [ga_id]
    )

    if not existing_step:
        # Сохраняем начальное состояние как первый шаг (индекс 0)
        db.execute_query(
            """INSERT INTO GAME_STEPS
                   (ID, SESSION_ID, ATTEMPT_ID, ACTION_ID, STATE_AFTER,
                    IS_ACTUAL, IS_IMPORT, IS_MARK, STEP_INDEX, STEP_TIME)
                   VALUES (SEQ_GAME_STEPS.NEXTVAL, :1, :2, :3, :4,
                           1, 0, 0, 0, SYSDATE)""",
            [gs_id, ga_id, action_move, shuffled_json]
        )
        print(f"Initial step (index 0) saved for attempt {ga_id}")
    else:
        print(f"Step 0 already exists for attempt {ga_id}")

    # Обновляем счетчик игр пользователя
    db.execute_query(
        "UPDATE USERS SET GAMES_COUNT = GAMES_COUNT + 1, "
        "FIRST_GAME_DATE = NVL(FIRST_GAME_DATE, SYSDATE) WHERE ID = :1",
        [user_id]
    )

    # Сохраняем ID сессии в Flask session
    session["game_session_id"] = gs_id

    # Логируем информацию о перемешивании
    misplaced_count = count_misplaced_tiles(shuffled_flat, target_flat)
    print(f"Game started: puzzle_id={puzzle_id}, difficulty={difficulty_name}, "
          f"grid_size={grid_size}, moves_done={moves_done}, "
          f"misplaced_tiles={misplaced_count}, seed={game_seed}")

    return redirect(url_for("game"))


# ================================================================
# ИГРА -- СТРАНИЦА
# ================================================================

@app.route("/game")
def game():
    if not get_current_user_id():
        return redirect(url_for("login"))

    gsid = get_active_session_id()
    if not gsid:
        return redirect(url_for("index"))

    # Дополнительная проверка активности сессии
    if not check_current_session_valid():
        return redirect(url_for("index"))

    attempt = get_active_attempt(gsid)
    if not attempt:
        session.pop("game_session_id", None)
        return redirect(url_for("index"))

    grid_size = attempt["grid_size"]
    board, flat = parse_board(read_clob(attempt["current_state"]), grid_size)
    _, tgt_flat = parse_board(read_clob(attempt["target_state"]), grid_size)
    misplaced, manhattan, _ = compute_metrics(flat, tgt_flat, grid_size)
    pct = progress_pct(attempt["initial_manhattan_distance"], manhattan)

    # Получаем лимит времени из таблицы PUZZLE_SIZES
    time_limit_row = db.fetch_one(
        "SELECT DEFAULT_TIME_LIMIT FROM PUZZLE_SIZES WHERE GRID_SIZE = :1",
        [grid_size]
    )

    # Преобразуем INTERVAL в секунды
    time_limit_seconds = 0
    if time_limit_row and time_limit_row["default_time_limit"]:
        # Парсим INTERVAL DAY TO SECOND
        time_limit_str = str(time_limit_row["default_time_limit"])
        # Пример формата: "+08 00:00:00" или "8 0:0:0"
        import re
        match = re.search(r'(\d+)\s+(\d+):(\d+):(\d+)', time_limit_str)
        if match:
            days = int(match.group(1))
            hours = int(match.group(2))
            minutes = int(match.group(3))
            seconds = int(match.group(4))
            time_limit_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
        else:
            # Если не удалось распарсить, ставим значение по умолчанию
            time_limit_seconds = grid_size * 60  # примерно N минут

    # Получаем время начала игры
    start_time_row = db.fetch_one(
        "SELECT STARTED_AT FROM GAME_ATTEMPTS WHERE ID = :1",
        [attempt["id"]]
    )

    elapsed_seconds = 0
    if start_time_row and start_time_row["started_at"]:
        # Вычисляем прошедшее время в секундах
        start_time = start_time_row["started_at"]
        if hasattr(start_time, 'timestamp'):
            # Если это datetime объект
            elapsed_seconds = int((datetime.now() - start_time).total_seconds())
        else:
            # Если это строка или другой формат
            elapsed_seconds = 0

    active = {
        "session_id": gsid,
        "id": attempt["id"],
        "grid_size": grid_size,
        "difficulty": attempt["difficulty"],
        "current_step": attempt["undo_pointer"],
        "misplaced_tiles": misplaced,
        "manhattan_distance": manhattan,
        "progress_pct": pct,
        "initial_manhattan": attempt["initial_manhattan_distance"],
        "time_limit_seconds": time_limit_seconds,
        "elapsed_seconds": elapsed_seconds,
    }

    return render_template(
        "game.html",
        active=active,
        board=board,
        username=session.get("username")
    )


# ================================================================
# ИГРА -- ХОД
# ================================================================

@app.route("/game/move", methods=["POST"])
def make_move():
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    # Проверяем валидность сессии
    if not check_current_session_valid():
        return jsonify({"error": "Сессия истекла по времени"}), 401

    tile = request.json.get("tile")
    if tile is None:
        return jsonify({"error": "Не указана плитка"}), 400

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    if not ensure_db_connection():
        return jsonify({"error": "Ошибка подключения к БД"}), 500

    attempt = get_active_attempt(gsid)
    if not attempt:
        return jsonify({"error": "Нет активной попытки"}), 400

    grid_size = attempt["grid_size"]

    current_state_json = read_clob(attempt["current_state"])
    target_state_json = read_clob(attempt["target_state"])

    board, flat = parse_board(current_state_json, grid_size)
    _, tgt_flat = parse_board(target_state_json, grid_size)

    try:
        tile_value = int(tile)
        tile_pos = flat.index(tile_value)
        empty_pos = flat.index(0)
    except ValueError:
        return jsonify({"error": f"Плитка {tile} не найдена"}), 400

    diff = tile_pos - empty_pos
    if diff not in (1, -1, grid_size, -grid_size):
        return jsonify({"error": "Недопустимый ход"}), 400
    if diff in (1, -1):
        if tile_pos // grid_size != empty_pos // grid_size:
            return jsonify({"error": "Недопустимый ход"}), 400

    flat[empty_pos], flat[tile_pos] = flat[tile_pos], flat[empty_pos]

    new_state_json = json.dumps(flat)

    misplaced, manhattan, _ = compute_metrics(flat, tgt_flat, grid_size)
    next_idx = attempt["undo_pointer"] + 1

    action_move_row = db.fetch_one("SELECT ID FROM ACTION_TYPES WHERE NAME='move'")
    if not action_move_row:
        db.execute_query("INSERT INTO ACTION_TYPES (ID, NAME) VALUES (SEQ_ACTION_TYPES.NEXTVAL, 'move')")
        action_move_row = db.fetch_one("SELECT ID FROM ACTION_TYPES WHERE NAME='move'")
    action_move = action_move_row["id"]

    # Проверяем, существует ли уже шаг с таким индексом
    existing_step = db.fetch_one(
        "SELECT ID FROM GAME_STEPS WHERE ATTEMPT_ID = :1 AND STEP_INDEX = :2",
        [attempt["id"], next_idx]
    )

    if existing_step:
        # Если шаг существует, просто обновляем его
        db.execute_query(
            """UPDATE GAME_STEPS 
               SET STATE_AFTER = :1, IS_ACTUAL = 1, STEP_TIME = SYSDATE,
                   TILE_VALUE = :2
               WHERE ATTEMPT_ID = :3 AND STEP_INDEX = :4""",
            [new_state_json, tile_value, attempt["id"], next_idx]
        )
        print(f"Updated existing step {next_idx}")
    else:
        # Если шага нет, вставляем новый
        db.execute_query(
            """INSERT INTO GAME_STEPS
               (ID, SESSION_ID, ATTEMPT_ID, ACTION_ID, TILE_VALUE,
                STATE_AFTER, IS_ACTUAL, IS_IMPORT, IS_MARK, STEP_INDEX, STEP_TIME)
               VALUES (SEQ_GAME_STEPS.NEXTVAL, :1, :2, :3, :4,
                       :5, 1, 0, 0, :6, SYSDATE)""",
            [gsid, attempt["id"], action_move, tile_value, new_state_json, next_idx]
        )
        print(f"Inserted new step {next_idx}")

    # Инвалидируем все шаги после текущего индекса
    db.execute_query(
        "UPDATE GAME_STEPS SET IS_ACTUAL = 0 WHERE ATTEMPT_ID = :1 AND STEP_INDEX > :2",
        [attempt["id"], next_idx]
    )

    db.execute_query(
        """UPDATE GAME_ATTEMPTS
           SET CURRENT_STATE = :1, CURRENT_MISPLACED_TILES = :2,
               CURRENT_MANHATTAN_DISTANCE = :3, UNDO_POINTER = :4
           WHERE ID = :5""",
        [new_state_json, misplaced, manhattan, next_idx, attempt["id"]]
    )

    # Обновляем время последней активности
    db.execute_query(
        "UPDATE GAME_SESSIONS SET STEPS_COUNT = STEPS_COUNT + 1, LAST_ACTIVITY_AT = SYSTIMESTAMP WHERE ID = :1",
        [gsid]
    )

    board = [flat[r * grid_size:(r + 1) * grid_size] for r in range(grid_size)]
    pct = progress_pct(attempt["initial_manhattan_distance"], manhattan)

    if check_win_condition(flat, tgt_flat):
        status_solved_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='solved'")
        if not status_solved_row:
            db.execute_query("INSERT INTO GAME_STATUSES (ID, NAME) VALUES (SEQ_GAME_STATUSES.NEXTVAL, 'solved')")
            status_solved_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='solved'")
        status_solved = status_solved_row["id"]

        db.execute_query(
            "UPDATE GAME_ATTEMPTS SET STATUS_ID = :1, FINISHED_AT = SYSTIMESTAMP WHERE ID = :2",
            [status_solved, attempt["id"]]
        )
        db.execute_query(
            "UPDATE GAME_SESSIONS SET STATUS_ID = :1, END_TIME = SYSDATE WHERE ID = :2",
            [status_solved, gsid]
        )
        session.pop("game_session_id", None)

        return jsonify({
            "status": "solved",
            "board": [flat[r * grid_size:(r + 1) * grid_size] for r in range(grid_size)],
            "steps": next_idx,
            "progress": 100,
            "misplaced": 0,
            "manhattan": 0
        })

    # Проверяем, есть ли шаги для REDO
    redo_check = db.fetch_one(
        "SELECT COUNT(*) AS CNT FROM GAME_STEPS WHERE ATTEMPT_ID = :1 AND STEP_INDEX > :2",
        [attempt["id"], next_idx]
    )
    redo_available = redo_check and redo_check["cnt"] > 0

    return jsonify({
        "status": "ok",
        "board": board,
        "steps": next_idx,
        "misplaced": misplaced,
        "manhattan": manhattan,
        "progress": pct,
        "undoAvailable": True,
        "redoAvailable": redo_available
    })


# ================================================================
# ИГРА -- UNDO
# ================================================================

@app.route("/game/undo", methods=["POST"])
def undo_move():
    print("=== UNDO CALLED ===")
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    # Проверяем валидность сессии
    if not check_current_session_valid():
        return jsonify({"error": "Сессия истекла по времени"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    if not ensure_db_connection():
        return jsonify({"error": "Потеряно соединение с БД"}), 500

    try:
        # Получаем данные игры
        game_data = db.fetch_one("""
            SELECT 
                GA.ID as attempt_id,
                GA.UNDO_POINTER,
                GA.INITIAL_MANHATTAN_DISTANCE,
                PS.GRID_SIZE,
                PZ.TARGET_STATE,
                GA.CURRENT_STATE
            FROM GAME_ATTEMPTS GA
            JOIN GAME_STATUSES GST ON GA.STATUS_ID = GST.ID
            JOIN PUZZLES PZ ON GA.PUZZLE_ID = PZ.ID
            JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
            WHERE GA.SESSION_ID = :1 AND GST.NAME = 'active' AND ROWNUM = 1
        """, [gsid])

        if not game_data:
            return jsonify({"error": "Активная игра не найдена"}), 400

        attempt_id = game_data["attempt_id"]
        current_pointer = game_data["undo_pointer"]
        init_manhattan = game_data["initial_manhattan_distance"]
        grid_size = game_data["grid_size"]
        target_state = game_data["target_state"]

        print(f"Current undo_pointer: {current_pointer}")

        # Проверяем, можно ли отменить ход
        if current_pointer <= 0:
            return jsonify({"error": "Нет ходов для отмены"}), 400

        # Получаем предыдущее состояние
        prev_idx = current_pointer - 1

        # Получаем состояние для предыдущего шага
        prev_state_row = db.fetch_one("""
            SELECT TO_CHAR(STATE_AFTER) as state_str 
            FROM GAME_STEPS 
            WHERE ATTEMPT_ID = :1 AND STEP_INDEX = :2
        """, [attempt_id, prev_idx])

        if not prev_state_row:
            print(f"ERROR: No previous state found for attempt {attempt_id}, step {prev_idx}")
            return jsonify({"error": f"Не найдено состояние для шага {prev_idx}"}), 400

        prev_state_str = prev_state_row["state_str"]

        # Проверяем, что состояние не пустое
        if not prev_state_str or prev_state_str.isspace():
            print(f"ERROR: Previous state is empty for attempt {attempt_id}, step {prev_idx}")
            return jsonify({"error": "Предыдущее состояние повреждено (пустое)"}), 500

        # Преобразуем target_state в строку
        target_state_str = read_clob(target_state)
        if not target_state_str or target_state_str.isspace():
            print("ERROR: Target state is empty")
            return jsonify({"error": "Целевое состояние повреждено"}), 500

        # Парсим состояния
        _, flat = parse_board(prev_state_str, grid_size)
        _, tgt_flat = parse_board(target_state_str, grid_size)

        if not flat:
            print(f"ERROR: Failed to parse board state: {prev_state_str[:100]}...")
            return jsonify({"error": "Ошибка парсинга состояния доски"}), 500

        if not tgt_flat:
            print("ERROR: Failed to parse target state")
            return jsonify({"error": "Ошибка парсинга целевого состояния"}), 500

        # Вычисляем метрики для предыдущего состояния
        misplaced, manhattan, _ = compute_metrics(flat, tgt_flat, grid_size)

        # Обновляем состояние в БД
        db.execute_query(
            """UPDATE GAME_ATTEMPTS 
               SET CURRENT_STATE = :1, 
                   CURRENT_MISPLACED_TILES = :2, 
                   CURRENT_MANHATTAN_DISTANCE = :3, 
                   UNDO_POINTER = :4 
               WHERE ID = :5""",
            [prev_state_str, misplaced, manhattan, prev_idx, attempt_id]
        )

        # Обновляем время активности сессии
        db.execute_query(
            "UPDATE GAME_SESSIONS SET LAST_ACTIVITY_AT = SYSTIMESTAMP WHERE ID = :1",
            [gsid]
        )

        # Формируем доску для ответа
        board = []
        for r in range(grid_size):
            row = flat[r * grid_size:(r + 1) * grid_size]
            board.append(row)

        pct = progress_pct(init_manhattan, manhattan)

        print(f"Undo successful, new step: {prev_idx}")

        # Проверяем, есть ли еще шаги для UNDO
        undo_available = prev_idx > 0

        # Проверяем, есть ли шаги для REDO (шаги после текущего указателя)
        redo_check = db.fetch_one(
            "SELECT COUNT(*) AS CNT FROM GAME_STEPS WHERE ATTEMPT_ID = :1 AND STEP_INDEX > :2",
            [attempt_id, prev_idx]
        )
        redo_available = redo_check and redo_check["cnt"] > 0

        return jsonify({
            "status": "ok",
            "board": board,
            "steps": prev_idx,
            "progress": pct,
            "misplaced": misplaced,
            "manhattan": manhattan,
            "undoAvailable": undo_available,
            "redoAvailable": redo_available
        })

    except Exception as e:
        print(f"UNDO error: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Ошибка: {str(e)}"}), 500


# ================================================================
# ИГРА -- REDO (ИСПРАВЛЕННАЯ ВЕРСИЯ)
# ================================================================

@app.route("/game/redo", methods=["POST"])
def redo_move():
    print("=== REDO CALLED ===")
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    # Проверяем валидность сессии
    if not check_current_session_valid():
        return jsonify({"error": "Сессия истекла по времени"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    if not ensure_db_connection():
        return jsonify({"error": "Потеряно соединение с БД"}), 500

    try:
        # Получаем данные игры - добавляем CURRENT_STATE для отладки
        game_data = db.fetch_one("""
            SELECT 
                GA.ID as attempt_id,
                GA.UNDO_POINTER,
                GA.INITIAL_MANHATTAN_DISTANCE,
                PS.GRID_SIZE,
                PZ.TARGET_STATE,
                GA.CURRENT_STATE
            FROM GAME_ATTEMPTS GA
            JOIN GAME_STATUSES GST ON GA.STATUS_ID = GST.ID
            JOIN PUZZLES PZ ON GA.PUZZLE_ID = PZ.ID
            JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
            WHERE GA.SESSION_ID = :1 AND GST.NAME = 'active' AND ROWNUM = 1
        """, [gsid])

        if not game_data:
            return jsonify({"error": "Активная игра не найдена"}), 400

        attempt_id = game_data["attempt_id"]
        current_pointer = game_data["undo_pointer"]
        init_manhattan = game_data["initial_manhattan_distance"]
        grid_size = game_data["grid_size"]
        target_state = game_data["target_state"]

        print(f"Current undo_pointer: {current_pointer}")

        next_idx = current_pointer + 1

        # Проверяем, есть ли следующий шаг
        next_state_row = db.fetch_one("""
            SELECT TO_CHAR(STATE_AFTER) as state_str 
            FROM GAME_STEPS 
            WHERE ATTEMPT_ID = :1 AND STEP_INDEX = :2
        """, [attempt_id, next_idx])

        if not next_state_row:
            print(f"No next state found for attempt {attempt_id}, step {next_idx}")
            return jsonify({"error": "Нет отменённых ходов"}), 400

        next_state_str = next_state_row["state_str"]

        if not next_state_str or next_state_str.isspace():
            print(f"ERROR: Next state is empty for attempt {attempt_id}, step {next_idx}")
            return jsonify({"error": "Следующее состояние повреждено (пустое)"}), 500

        # Преобразуем target_state в строку - ИСПРАВЛЕНО
        target_state_str = read_clob(target_state)

        # Если target_state_str пустой или содержит только пробелы, пробуем получить его из PUZZLES еще раз
        if not target_state_str or target_state_str.isspace():
            print("WARNING: Target state is empty, trying to fetch again...")
            # Пробуем получить целевое состояние напрямую из PUZZLES
            target_direct = db.fetch_one("""
                SELECT TO_CHAR(TARGET_STATE) as target_str 
                FROM PUZZLES PZ
                JOIN GAME_ATTEMPTS GA ON GA.PUZZLE_ID = PZ.ID
                WHERE GA.ID = :1
            """, [attempt_id])
            if target_direct and target_direct["target_str"]:
                target_state_str = target_direct["target_str"]
                print(f"Successfully fetched target state directly: {target_state_str[:50]}...")
            else:
                print("ERROR: Still cannot get target state")
                return jsonify({"error": "Целевое состояние повреждено"}), 500

        # Парсим состояния
        _, flat = parse_board(next_state_str, grid_size)
        _, tgt_flat = parse_board(target_state_str, grid_size)

        if not flat:
            print(f"ERROR: Failed to parse board state: {next_state_str[:100]}...")
            return jsonify({"error": "Ошибка парсинга состояния доски"}), 500

        if not tgt_flat:
            print(f"ERROR: Failed to parse target state: {target_state_str[:100]}...")
            return jsonify({"error": "Ошибка парсинга целевого состояния"}), 500

        misplaced, manhattan, _ = compute_metrics(flat, tgt_flat, grid_size)

        # Обновляем состояние в БД
        db.execute_query(
            """UPDATE GAME_ATTEMPTS 
               SET CURRENT_STATE = :1, 
                   CURRENT_MISPLACED_TILES = :2, 
                   CURRENT_MANHATTAN_DISTANCE = :3, 
                   UNDO_POINTER = :4 
               WHERE ID = :5""",
            [next_state_str, misplaced, manhattan, next_idx, attempt_id]
        )

        db.execute_query(
            "UPDATE GAME_SESSIONS SET LAST_ACTIVITY_AT = SYSTIMESTAMP WHERE ID = :1",
            [gsid]
        )

        # Проверяем, есть ли еще шаги для REDO
        has_more_row = db.fetch_one(
            "SELECT COUNT(*) AS CNT FROM GAME_STEPS WHERE ATTEMPT_ID = :1 AND STEP_INDEX > :2",
            [attempt_id, next_idx]
        )
        redo_available = has_more_row and has_more_row["cnt"] > 0

        board = []
        for r in range(grid_size):
            row = flat[r * grid_size:(r + 1) * grid_size]
            board.append(row)

        pct = progress_pct(init_manhattan, manhattan)

        print(f"Redo successful, new step: {next_idx}, more redo: {redo_available}")

        return jsonify({
            "status": "ok",
            "board": board,
            "steps": next_idx,
            "progress": pct,
            "misplaced": misplaced,
            "manhattan": manhattan,
            "undoAvailable": True,  # После REDO всегда можно сделать UNDO
            "redoAvailable": redo_available
        })

    except Exception as e:
        print(f"REDO error: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Ошибка: {str(e)}"}), 500


# ================================================================
# ИГРА -- ПОДСКАЗКА
# ================================================================

@app.route("/game/hint", methods=["POST"])
def get_hint():
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    # Проверяем валидность сессии
    if not check_current_session_valid():
        return jsonify({"error": "Сессия истекла по времени"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    try:
        attempt = get_active_attempt(gsid)
        if not attempt:
            return jsonify({"error": "Нет активной попытки"}), 400

        grid_size = attempt["grid_size"]
        current_state_json = read_clob(attempt["current_state"])
        target_state_json = read_clob(attempt["target_state"])

        _, flat = parse_board(current_state_json, grid_size)
        _, tgt_flat = parse_board(target_state_json, grid_size)

        misplaced, manhattan, correct = compute_metrics(flat, tgt_flat, grid_size)
        pct = progress_pct(attempt["initial_manhattan_distance"], manhattan)

        return jsonify({
            "misplaced": misplaced,
            "manhattan": manhattan,
            "correct": correct,
            "progress": pct
        })
    except Exception as e:
        print(f"HINT error: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================================================================
# ИГРА -- ЗАВЕРШИТЬ
# ================================================================

@app.route("/game/over", methods=["POST"])
def game_over():
    if not get_current_user_id():
        return redirect(url_for("login"))

    gsid = get_active_session_id()
    if gsid:
        # Проверяем, активна ли сессия
        active_check = db.fetch_one(
            """SELECT GS.ID 
               FROM GAME_SESSIONS GS
               JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
               WHERE GS.ID = :1 AND GST.NAME = 'active'""",
            [gsid]
        )

        if active_check:
            status_abandoned_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'")
            if not status_abandoned_row:
                db.execute_query("INSERT INTO GAME_STATUSES (ID, NAME) VALUES (SEQ_GAME_STATUSES.NEXTVAL, 'abandoned')")
                status_abandoned_row = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'")
            status_abandoned = status_abandoned_row["id"]

            db.execute_query(
                "UPDATE GAME_ATTEMPTS SET STATUS_ID=:1, FINISHED_AT=SYSTIMESTAMP WHERE SESSION_ID=:2",
                [status_abandoned, gsid]
            )
            db.execute_query(
                "UPDATE GAME_SESSIONS SET STATUS_ID=:1, END_TIME=SYSDATE WHERE ID=:2",
                [status_abandoned, gsid]
            )

        session.pop("game_session_id", None)

    return redirect(url_for("index"))


# ================================================================
# ИГРА -- ПЕРЕЗАПУСК
# ================================================================

@app.route("/game/restart", methods=["POST"])
def restart_game():
    """Перезапускает текущую игру с тем же пазлом, но новым перемешиванием"""
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    try:
        # Получаем ID пазла из текущей сессии
        puzzle_data = db.fetch_one(
            "SELECT PUZZLE_ID FROM GAME_SESSIONS WHERE ID = :1",
            [gsid]
        )

        if not puzzle_data:
            return jsonify({"error": "Сессия не найдена"}), 404

        puzzle_id = puzzle_data["puzzle_id"]

        # Завершаем текущую сессию
        status_abandoned = db.fetch_one("SELECT ID FROM GAME_STATUSES WHERE NAME='abandoned'")
        if status_abandoned:
            db.execute_query(
                "UPDATE GAME_ATTEMPTS SET STATUS_ID = :1, FINISHED_AT = SYSTIMESTAMP WHERE SESSION_ID = :2",
                [status_abandoned["id"], gsid]
            )
            db.execute_query(
                "UPDATE GAME_SESSIONS SET STATUS_ID = :1, END_TIME = SYSDATE WHERE ID = :2",
                [status_abandoned["id"], gsid]
            )

        session.pop("game_session_id", None)

        # Запускаем новую игру с тем же пазлом
        return redirect(url_for("start_game", puzzle_id=puzzle_id))

    except Exception as e:
        print(f"RESTART error: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================================================================
# ДИАГНОСТИЧЕСКАЯ ФУНКЦИЯ
# ================================================================

@app.route("/game/diagnose", methods=["GET"])
def diagnose_game():
    """Диагностическая функция для проверки состояния игры"""
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    try:
        # Получаем информацию о попытке и пазле
        data = db.fetch_one("""
            SELECT 
                GA.ID as attempt_id,
                GA.UNDO_POINTER,
                PS.GRID_SIZE,
                DL.NAME as DIFFICULTY,
                SUBSTR(TO_CHAR(GA.CURRENT_STATE), 1, 100) as current_state_preview,
                SUBSTR(TO_CHAR(PZ.TARGET_STATE), 1, 100) as target_state_preview,
                PZ.ID as puzzle_id
            FROM GAME_ATTEMPTS GA
            JOIN GAME_STATUSES GST ON GA.STATUS_ID = GST.ID
            JOIN PUZZLES PZ ON GA.PUZZLE_ID = PZ.ID
            JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
            JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
            WHERE GA.SESSION_ID = :1 AND GST.NAME = 'active' AND ROWNUM = 1
        """, [gsid])

        # Получаем все шаги
        steps = db.fetch_all("""
            SELECT STEP_INDEX, IS_ACTUAL, TILE_VALUE,
                   SUBSTR(TO_CHAR(STATE_AFTER), 1, 50) as state_preview
            FROM GAME_STEPS
            WHERE ATTEMPT_ID = :1
            ORDER BY STEP_INDEX
        """, [data["attempt_id"]])

        return jsonify({
            "session_id": gsid,
            "attempt_id": data["attempt_id"],
            "puzzle_id": data["puzzle_id"],
            "grid_size": data["grid_size"],
            "difficulty": data["difficulty"],
            "undo_pointer": data["undo_pointer"],
            "current_state_preview": data["current_state_preview"],
            "target_state_preview": data["target_state_preview"],
            "steps": steps,
            "steps_count": len(steps)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ================================================================
# ОТЛАДОЧНАЯ ФУНКЦИЯ
# ================================================================

@app.route("/game/debug", methods=["GET"])
def debug_game():
    """Отладочная функция для проверки состояния игры"""
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    gsid = get_active_session_id()
    if not gsid:
        return jsonify({"error": "Нет активной игровой сессии"}), 400

    try:
        # Получаем информацию о попытке
        attempt_data = db.fetch_one("""
            SELECT GA.ID, GA.UNDO_POINTER, 
                   SUBSTR(TO_CHAR(GA.CURRENT_STATE), 1, 100) as CURRENT_STATE_PREVIEW,
                   GS.STEPS_COUNT
            FROM GAME_ATTEMPTS GA
            JOIN GAME_SESSIONS GS ON GA.SESSION_ID = GS.ID
            WHERE GA.SESSION_ID = :1 AND ROWNUM = 1
        """, [gsid])

        # Получаем все шаги
        steps = db.fetch_all("""
            SELECT STEP_INDEX, IS_ACTUAL, TILE_VALUE,
                   SUBSTR(TO_CHAR(STATE_AFTER), 1, 50) as STATE_PREVIEW
            FROM GAME_STEPS
            WHERE ATTEMPT_ID = :1
            ORDER BY STEP_INDEX
        """, [attempt_data["id"]])

        return jsonify({
            "session_id": gsid,
            "attempt_id": attempt_data["id"],
            "undo_pointer": attempt_data["undo_pointer"],
            "steps_count": len(steps),
            "steps": steps
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ================================================================
# ТАБЛИЦА ЛИДЕРОВ
# ================================================================

@app.route("/leaderboard")
def leaderboard():
    if not get_current_user_id():
        return redirect(url_for("login"))

    players = db.fetch_all(
        """SELECT U.ID, U.USERNAME,
                  COUNT(DISTINCT GS.ID) AS TOTAL_GAMES,
                  SUM(CASE WHEN GST.NAME = 'solved' THEN 1 ELSE 0 END) AS SOLVED_GAMES,
                  CASE 
                      WHEN COUNT(DISTINCT GS.ID) > 0 
                      THEN ROUND(SUM(CASE WHEN GST.NAME = 'solved' THEN 1 ELSE 0 END) / 
                           COUNT(DISTINCT GS.ID) * 100, 1)
                      ELSE 0 
                  END AS SUCCESS_RATE,
                  ROUND(AVG(CASE WHEN GST.NAME = 'solved'
                            THEN (GS.END_TIME - GS.START_TIME) * 24 * 60 END), 1) AS AVG_TIME_MINUTES,
                  MIN(CASE WHEN GST.NAME = 'solved' THEN GS.STEPS_COUNT END) AS BEST_STEPS
           FROM USERS U
           LEFT JOIN GAME_SESSIONS GS ON U.ID = GS.USER_ID
           LEFT JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
           GROUP BY U.ID, U.USERNAME
           ORDER BY SUCCESS_RATE DESC, SOLVED_GAMES DESC, AVG_TIME_MINUTES NULLS LAST"""
    )
    return render_template("leaderboard.html", players=players, username=session.get("username"))


# ================================================================
# ИСТОРИЯ
# ================================================================

@app.route("/history")
def history():
    if not get_current_user_id():
        return redirect(url_for("login"))

    user_id = get_current_user_id()
    games = db.fetch_all(
        """SELECT GS.ID AS SESSION_ID, GS.START_TIME, GS.END_TIME,
                  GST.NAME AS STATUS, GS.STEPS_COUNT,
                  PS.GRID_SIZE, DL.NAME AS DIFFICULTY,
                  ROUND((GS.END_TIME - GS.START_TIME)*24*60, 1) AS TIME_MINUTES
           FROM GAME_SESSIONS GS
           JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
           JOIN PUZZLES PZ ON GS.PUZZLE_ID = PZ.ID
           JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
           WHERE GS.USER_ID = :1 AND GST.NAME != 'active'
           ORDER BY GS.START_TIME DESC""",
        [user_id]
    )
    return render_template("history.html", games=games, username=session.get("username"))


# ================================================================
# ИСТОРИЯ -- ДЕТАЛИ ИГРЫ (AJAX)
# ================================================================

@app.route("/history/game/<int:session_id>")
def game_details(session_id):
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    user_id = get_current_user_id()

    # Проверяем что эта игра принадлежит текущему пользователю
    game = db.fetch_one(
        """SELECT GS.ID, GS.START_TIME, GS.END_TIME,
                  GST.NAME AS STATUS, GS.STEPS_COUNT,
                  PS.GRID_SIZE, DL.NAME AS DIFFICULTY,
                  ROUND((GS.END_TIME - GS.START_TIME)*24*60, 1) AS TIME_MINUTES,
                  PZ.SEED
           FROM GAME_SESSIONS GS
           JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
           JOIN PUZZLES PZ ON GS.PUZZLE_ID = PZ.ID
           JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
           WHERE GS.ID = :1 AND GS.USER_ID = :2""",
        [session_id, user_id]
    )

    if not game:
        return jsonify({"error": "Игра не найдена"}), 404

    # История ходов
    steps = db.fetch_all(
        """SELECT GS.STEP_INDEX, AT.NAME AS ACTION,
                  GS.TILE_VALUE, GS.DIRECTION, GS.STEP_TIME, GS.IS_ACTUAL
           FROM GAME_STEPS GS
           JOIN ACTION_TYPES AT ON GS.ACTION_ID = AT.ID
           WHERE GS.SESSION_ID = :1 AND GS.IS_ACTUAL = 1 AND GS.STEP_INDEX > 0
           ORDER BY GS.STEP_INDEX""",
        [session_id]
    )

    # Форматируем данные
    start_time = game["start_time"]
    end_time = game["end_time"]

    def fmt_date(d):
        if not d:
            return None
        if hasattr(d, "strftime"):
            return d.strftime("%d.%m.%Y %H:%M")
        return str(d)

    def fmt_time(d):
        if not d:
            return None
        if hasattr(d, "strftime"):
            return d.strftime("%H:%M:%S")
        return str(d)

    steps_list = []
    for s in steps:
        step_time = s["step_time"]
        steps_list.append({
            "index": s["step_index"],
            "action": s["action"],
            "tile": s["tile_value"],
            "direction": s["direction"],
            "time": fmt_time(step_time),
        })

    return jsonify({
        "session_id": session_id,
        "start_time": fmt_date(start_time),
        "end_time": fmt_date(end_time),
        "status": game["status"],
        "steps_count": game["steps_count"],
        "grid_size": game["grid_size"],
        "difficulty": game["difficulty"],
        "time_minutes": game["time_minutes"],
        "seed": game["seed"],
        "steps": steps_list,
    })


# ================================================================
# ИСТОРИЯ -- ЭКСПОРТ ИГРЫ (скачать JSON)
# ================================================================

@app.route("/history/export/<int:session_id>")
def export_game(session_id):
    if not get_current_user_id():
        return jsonify({"error": "Не авторизован"}), 401

    user_id = get_current_user_id()

    game = db.fetch_one(
        """SELECT GS.ID, GS.START_TIME, GS.END_TIME,
                  GST.NAME AS STATUS, GS.STEPS_COUNT,
                  PS.GRID_SIZE, DL.NAME AS DIFFICULTY,
                  PZ.SEED, PZ.TARGET_STATE
           FROM GAME_SESSIONS GS
           JOIN GAME_STATUSES GST ON GS.STATUS_ID = GST.ID
           JOIN PUZZLES PZ ON GS.PUZZLE_ID = PZ.ID
           JOIN PUZZLE_SIZES PS ON PZ.PUZZLE_SIZE_ID = PS.ID
           JOIN DIFFICULTY_LEVELS DL ON PZ.DIFFICULTY_ID = DL.ID
           WHERE GS.ID = :1 AND GS.USER_ID = :2""",
        [session_id, user_id]
    )

    if not game:
        return jsonify({"error": "Игра не найдена"}), 404

    steps = db.fetch_all(
        """SELECT GS.STEP_INDEX, AT.NAME AS ACTION,
                  GS.TILE_VALUE, GS.DIRECTION,
                  TO_CHAR(GS.STATE_AFTER) AS STATE_AFTER, GS.STEP_TIME
           FROM GAME_STEPS GS
           JOIN ACTION_TYPES AT ON GS.ACTION_ID = AT.ID
           WHERE GS.SESSION_ID = :1 AND GS.IS_ACTUAL = 1
           ORDER BY GS.STEP_INDEX""",
        [session_id]
    )

    def fmt(d):
        if not d:
            return None
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d %H:%M:%S")
        return str(d)

    export_data = {
        "game": {
            "session_id": session_id,
            "seed": game["seed"],
            "grid_size": game["grid_size"],
            "difficulty": game["difficulty"],
            "status": game["status"],
            "steps_count": game["steps_count"],
            "start_time": fmt(game["start_time"]),
            "end_time": fmt(game["end_time"]),
        },
        "moves": [
            {
                "step": s["step_index"],
                "action": s["action"],
                "tile": s["tile_value"],
                "direction": s["direction"],
                "state": s["state_after"],
                "time": fmt(s["step_time"]),
            }
            for s in steps
        ]
    }

    from flask import Response
    import json as json_module
    response = Response(
        json_module.dumps(export_data, ensure_ascii=False, indent=2),
        mimetype="application/json",
        headers={
            "Content-Disposition": f"attachment; filename=game_{session_id}.json"
        }
    )
    return response


# ================================================================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ================================================================

if __name__ == "__main__":
    app.last_cleanup = time.time()  # Инициализируем время последней очистки
    app.run(debug=True, port=5000)