# HTML шаблон для страницы входа
LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ozon Parser - {{ store_name }} | Вход</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0a0e1a 0%, #141824 100%);
            color: #e0e0e0;
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }

        .login-container {
            max-width: 450px;
            width: 100%;
            background: #141824;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
            border: 1px solid #2a2f3f;
        }

        .logo {
            text-align: center;
            margin-bottom: 30px;
        }

        .store-badge {
            display: inline-block;
            background: linear-gradient(135deg, #2ecc71, #27ae60);
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
            margin-bottom: 15px;
        }

        .logo h1 {
            font-size: 28px;
            color: #fff;
            margin-bottom: 10px;
        }

        .logo p {
            color: #7a8099;
            font-size: 14px;
        }

        .input-group {
            margin-bottom: 25px;
        }

        .input-group label {
            display: block;
            margin-bottom: 8px;
            color: #7a8099;
            font-size: 14px;
        }

        .input-group input {
            width: 100%;
            padding: 14px;
            background: #0d1120;
            border: 1px solid #2a2f3f;
            border-radius: 12px;
            color: #e0e0e0;
            font-size: 16px;
            font-family: monospace;
            transition: all 0.2s ease;
        }

        .input-group input:focus {
            outline: none;
            border-color: #2ecc71;
            box-shadow: 0 0 10px rgba(46, 204, 113, 0.2);
        }

        .btn-login {
            width: 100%;
            padding: 14px;
            background: linear-gradient(135deg, #2ecc71, #27ae60);
            border: none;
            border-radius: 12px;
            color: white;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s ease;
        }

        .btn-login:hover {
            transform: translateY(-2px);
        }

        .error-message {
            background: rgba(231, 76, 60, 0.1);
            border: 1px solid rgba(231, 76, 60, 0.3);
            border-radius: 10px;
            padding: 12px;
            margin-bottom: 20px;
            color: #e74c3c;
            font-size: 14px;
            text-align: center;
        }

        .info-text {
            text-align: center;
            margin-top: 20px;
            font-size: 12px;
            color: #4a5069;
        }

        .info-text code {
            background: #0d1120;
            padding: 4px 8px;
            border-radius: 6px;
            font-family: monospace;
        }

        .server-info {
            background: #0d1120;
            border-radius: 10px;
            padding: 12px;
            margin-top: 20px;
            font-size: 12px;
            text-align: center;
            color: #5a6079;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="logo">
            <div class="store-badge">🏪 {{ store_name }}</div>
            <h1>🚀 Ozon Parser</h1>
            <p>Введите ключ доступа для входа</p>
        </div>

        {% if error %}
        <div class="error-message">
            ⚠️ {{ error }}
        </div>
        {% endif %}

        <form method="POST" action="/login">
            <div class="input-group">
                <label>🔑 Ключ доступа</label>
                <input type="password" name="access_key" placeholder="Введите ключ доступа" autofocus>
            </div>

            <button type="submit" class="btn-login">Войти в панель управления</button>
        </form>

        <div class="info-text">
            💡 Ключ доступа находится в Google Sheets<br>
            Лист <code>Настройки_{{ store_name }}</code>, строка <code>Ключ доступа</code>
        </div>

        <div class="server-info">
            🌐 Сервер: {{ server_url }}
        </div>
    </div>
</body>
</html>
'''

# HTML шаблон для дашборда
DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ozon Parser - {{ store_name }} | Панель управления</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #0a0e1a;
            color: #e0e0e0;
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 700px;
            margin: 0 auto;
            background: #141824;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
            border: 1px solid #2a2f3f;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 15px;
            flex-wrap: wrap;
        }

        .store-badge {
            background: linear-gradient(135deg, #2ecc71, #27ae60);
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: bold;
            color: white;
        }

        h1 {
            font-size: 24px;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .logout-btn {
            background: #2a2f3f;
            padding: 8px 16px;
            border-radius: 8px;
            color: #e0e0e0;
            text-decoration: none;
            font-size: 14px;
            transition: background 0.2s ease;
        }

        .logout-btn:hover {
            background: #3a4055;
        }

        .server-badge {
            background: #0d1120;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            color: #7a8099;
            font-family: monospace;
        }

        .subtitle {
            color: #7a8099;
            margin-bottom: 30px;
            font-size: 13px;
            border-bottom: 1px solid #2a2f3f;
            padding-bottom: 15px;
        }

        .status-card {
            background: #1a1f2e;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            text-align: center;
            border: 1px solid #2a2f3f;
        }

        .status-label {
            font-size: 14px;
            color: #7a8099;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }

        .status-value {
            font-size: 32px;
            font-weight: bold;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
        }

        .status-dot {
            width: 14px;
            height: 14px;
            border-radius: 50%;
            display: inline-block;
        }

        .status-idle .status-dot { background: #f39c12; box-shadow: 0 0 10px #f39c12; }
        .status-running .status-dot { background: #2ecc71; box-shadow: 0 0 10px #2ecc71; animation: pulse 1.5s infinite; }
        .status-stopped .status-dot { background: #e74c3c; box-shadow: 0 0 10px #e74c3c; }

        .status-idle .status-value { color: #f39c12; }
        .status-running .status-value { color: #2ecc71; }
        .status-stopped .status-value { color: #e74c3c; }

        .last-run {
            margin-top: 15px;
            font-size: 12px;
            color: #5a6079;
        }

        .message {
            margin-top: 10px;
            font-size: 12px;
            color: #7a8099;
            background: #0d1120;
            padding: 8px 12px;
            border-radius: 8px;
        }

        .buttons {
            display: flex;
            gap: 15px;
            margin-bottom: 25px;
            flex-wrap: wrap;
        }

        .btn {
            flex: 1;
            padding: 14px 20px;
            border: none;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            font-family: inherit;
            min-width: 120px;
        }

        .btn-primary {
            background: linear-gradient(135deg, #2ecc71, #27ae60);
            color: white;
        }

        .btn-primary:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(46, 204, 113, 0.3);
        }

        .btn-danger {
            background: linear-gradient(135deg, #e74c3c, #c0392b);
            color: white;
        }

        .btn-danger:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(231, 76, 60, 0.3);
        }

        .btn-warning {
            background: linear-gradient(135deg, #f39c12, #e67e22);
            color: white;
        }

        .btn-warning:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(243, 156, 18, 0.3);
        }

        .btn-secondary {
            background: #2a2f3f;
            color: #e0e0e0;
        }

        .btn-secondary:hover:not(:disabled) {
            background: #3a4055;
        }

        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }

        .info-panel {
            background: #0d1120;
            border-radius: 12px;
            padding: 15px;
            margin-top: 20px;
        }

        .info-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #1a1f2e;
            font-size: 13px;
        }

        .info-row:last-child {
            border-bottom: none;
        }

        .info-label {
            color: #7a8099;
        }

        .info-value {
            color: #e0e0e0;
            font-family: monospace;
        }

        .footer {
            text-align: center;
            margin-top: 20px;
            font-size: 11px;
            color: #4a5069;
        }

        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #2ecc71;
            color: white;
            padding: 12px 20px;
            border-radius: 10px;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.3s ease;
            z-index: 1000;
        }

        .toast.error {
            background: #e74c3c;
        }

        .toast.show {
            opacity: 1;
        }

        .loading {
            opacity: 0.7;
            pointer-events: none;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        @media (max-width: 600px) {
            .container { padding: 20px; }
            .btn { padding: 10px 15px; font-size: 14px; }
            .status-value { font-size: 24px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <div class="store-badge">🏪 {{ store_name }}</div>
                <h1>🚀 Ozon Parser</h1>
            </div>
            <div style="display: flex; gap: 10px; align-items: center;">
                <span class="server-badge">{{ server_url }}</span>
                <a href="/logout" class="logout-btn">🚪 Выйти</a>
            </div>
        </div>
        <div class="subtitle">Управление парсингом товаров</div>

        <div class="status-card {{status_class}}" id="statusCard">
            <div class="status-label">Текущий статус</div>
            <div class="status-value">
                <span class="status-dot"></span>
                <span id="statusName">{{status_name}}</span>
            </div>
            <div class="last-run" id="lastRun">
                {% if last_run %}
                Последний запуск: {{last_run}}
                {% else %}
                Нет данных о запусках
                {% endif %}
            </div>
            <div class="message" id="statusMessage">
                {{message if message else 'Система работает в штатном режиме'}}
            </div>
        </div>

        <div class="buttons">
            <button class="btn btn-primary" id="startBtn" {% if status == 'running' %}disabled{% endif %}>
                ▶ Запустить
            </button>
            <button class="btn btn-danger" id="stopBtn" {% if status != 'running' %}disabled{% endif %}>
                ⏹ Остановить
            </button>
            <button class="btn btn-secondary" id="restartBtn">
                🔄 Перезапустить
            </button>
        </div>

        <div class="buttons">
            <button class="btn btn-warning" id="enableAutoBtn" {% if status != 'stopped' %}disabled{% endif %}>
                🔓 Включить автозапуск
            </button>
            <button class="btn btn-secondary" id="disableAutoBtn" {% if status == 'stopped' %}disabled{% endif %}>
                🔒 Выключить автозапуск
            </button>
        </div>

        <div class="info-panel">
            <div class="info-row">
                <span class="info-label">Магазин</span>
                <span class="info-value">{{ store_name }}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Интервал автозапуска</span>
                <span class="info-value">{{ interval }} секунд</span>
            </div>
            <div class="info-row">
                <span class="info-label">Версия</span>
                <span class="info-value">1.0.0</span>
            </div>
        </div>

        <div class="footer">
            🔐 Доступ защищен ключом авторизации
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        let autoRefreshInterval = null;

        function getAccessKey() {
            return localStorage.getItem('access_key') || '';
        }

        function showToast(message, isError = false) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast' + (isError ? ' error' : '');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 3000);
        }

        async function apiRequest(url, options = {}) {
            const accessKey = getAccessKey();
            const response = await fetch(url, {
                ...options,
                headers: {
                    'Content-Type': 'application/json',
                    'X-Access-Key': accessKey,
                    ...options.headers
                }
            });

            if (response.status === 401) {
                window.location.href = '/login?expired=1';
                return null;
            }

            return response;
        }

        async function updateStatus() {
            try {
                const response = await apiRequest('/api/status');
                if (!response) return;

                const data = await response.json();

                const statusName = document.getElementById('statusName');
                const statusCard = document.getElementById('statusCard');
                const startBtn = document.getElementById('startBtn');
                const stopBtn = document.getElementById('stopBtn');
                const enableAutoBtn = document.getElementById('enableAutoBtn');
                const disableAutoBtn = document.getElementById('disableAutoBtn');
                const statusMessage = document.getElementById('statusMessage');
                const lastRun = document.getElementById('lastRun');

                statusName.textContent = data.status_name;
                statusCard.className = 'status-card ' + data.status_class;

                if (data.status === 'running') {
                    startBtn.disabled = true;
                    stopBtn.disabled = false;
                    enableAutoBtn.disabled = true;
                    disableAutoBtn.disabled = false;
                } else if (data.status === 'idle') {
                    startBtn.disabled = false;
                    stopBtn.disabled = true;
                    enableAutoBtn.disabled = true;
                    disableAutoBtn.disabled = false;
                } else if (data.status === 'stopped') {
                    startBtn.disabled = false;
                    stopBtn.disabled = true;
                    enableAutoBtn.disabled = false;
                    disableAutoBtn.disabled = true;
                }

                if (data.last_run) {
                    lastRun.innerHTML = 'Последний запуск: ' + data.last_run;
                } else {
                    lastRun.innerHTML = 'Нет данных о запусках';
                }

                if (data.message) {
                    statusMessage.textContent = data.message;
                } else if (data.status === 'idle') {
                    statusMessage.textContent = 'Ожидание следующего автоматического запуска';
                } else if (data.status === 'running') {
                    statusMessage.textContent = 'Парсинг выполняется... Пожалуйста, подождите';
                } else if (data.status === 'stopped') {
                    statusMessage.textContent = 'Автозапуск отключен. Нажмите "Включить автозапуск" для активации';
                }

            } catch (error) {
                console.error('Ошибка получения статуса:', error);
            }
        }

        async function sendCommand(command) {
            const container = document.querySelector('.container');
            container.classList.add('loading');

            try {
                const response = await apiRequest('/api/control', {
                    method: 'POST',
                    body: JSON.stringify({ command: command })
                });

                if (!response) return;

                const data = await response.json();

                if (data.success) {
                    showToast(data.message);
                    await updateStatus();
                } else {
                    showToast(data.message || 'Ошибка выполнения команды', true);
                }
            } catch (error) {
                showToast('Ошибка связи с сервером', true);
                console.error('Error:', error);
            } finally {
                container.classList.remove('loading');
            }
        }

        document.getElementById('startBtn')?.addEventListener('click', () => sendCommand('start'));
        document.getElementById('stopBtn')?.addEventListener('click', () => sendCommand('stop'));
        document.getElementById('restartBtn')?.addEventListener('click', () => sendCommand('restart'));
        document.getElementById('enableAutoBtn')?.addEventListener('click', () => sendCommand('enable_auto'));
        document.getElementById('disableAutoBtn')?.addEventListener('click', () => sendCommand('disable_auto'));

        updateStatus();
        autoRefreshInterval = setInterval(updateStatus, 3000);

        window.addEventListener('beforeunload', () => {
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
            }
        });
    </script>
</body>
</html>
'''


def get_login_html(error=None, server_url=None, store_name=None):
    """Генерирует HTML для страницы входа"""
    from jinja2 import Template
    template = Template(LOGIN_TEMPLATE)
    return template.render(error=error, server_url=server_url or 'Неизвестно', store_name=store_name or 'Магазин')


def get_dashboard_html(status_data: dict, interval: int, server_url: str, store_name: str):
    """Генерирует HTML для дашборда"""
    from status_manager import STATUS_NAMES, STATUS_CLASSES

    status = status_data.get('status', 'idle')

    context = {
        'status_name': STATUS_NAMES.get(status, 'Неизвестно'),
        'status_class': STATUS_CLASSES.get(status, ''),
        'status': status,
        'message': status_data.get('message', ''),
        'last_run': status_data.get('last_run', ''),
        'interval': interval,
        'server_url': server_url,
        'store_name': store_name
    }

    from jinja2 import Template
    template = Template(DASHBOARD_TEMPLATE)
    return template.render(**context)