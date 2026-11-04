"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const numberFormat = new Intl.NumberFormat("ru-RU");
  const pageNames = { overview: "Обзор", queue: "Очередь задач", sources: "Исходные папки", schedules: "Планировщик", settings: "Настройки" };
  const busyPhases = new Set(["login", "logout", "scanning", "checking", "uploading", "pausing", "reconciling", "quota", "deleting"]);
  const phaseInfo = {
    idle: ["Готов к работе", "Ожидание", "neutral", "cloud"],
    login: ["Подключаем аккаунт", "Подключение", "blue", "lock"],
    logout: ["Выходим из Mail.ru", "Выход", "orange", "logout"],
    scanning: ["Ищем фото и видео", "Сканирование", "blue", "scan"],
    checking: ["Проверяем доступ к Облаку", "Проверка", "blue", "lock"],
    reconciling: ["Проверяем файлы в Облаке", "Проверка", "blue", "scan"],
    deleting: ["Удаляем выбранные задачи", "Удаление", "orange", "remove"],
    quota: ["Обновляем данные о месте", "Обновление", "blue", "cloud"],
    uploading: ["Загружаем файлы", "Загрузка", "blue", "cloud"],
    pausing: ["Сохраняем прогресс", "Останавливаем", "orange", "pause"],
    paused: ["Загрузка приостановлена", "На паузе", "orange", "pause"],
    completed: ["Обработка завершена", "Завершено", "green", "check"],
    error: ["Нужно ваше внимание", "Приостановлено", "red", "alert"]
  };
  const jobStates = {
    pending: ["Ожидает", "neutral"], running: ["Загружается", "blue"], uploaded: ["Выполнено", "green"],
    failed: ["Ошибка", "red"], skipped: ["Пропущено", "orange"], paused: ["На паузе", "orange"]
  };
  const jobsSortFields = { name: "Имя файла", progress: "Прогресс", status: "Статус", size: "Размер", created_at: "Создана", completed_at: "Выполнена" };
  const jobsPageSizes = [25, 50, 100, 200];
  const jobsPreferences = (() => {
    try {
      const value = JSON.parse(localStorage.getItem("mail-cloud.queue.v2") || "{}");
      return value && typeof value === "object" && !Array.isArray(value) ? value : {};
    } catch { return {}; }
  })();
  const planReasons = {
    size_limit: "Размер больше 2 ГБ: требуется обычное хранилище.",
    documented_format: "Формат указан в справке автозагрузки; итоговый режим подтвердит Облако.",
    account_confirmed_mov: "MOV проверен в аккаунте; итоговый режим каждого файла подтвердит Облако.",
    outside_documented_formats: "Формат вне подтверждённого списка: запланирована загрузка в квоту."
  };
  const transferStages = { hash: "Проверка файла", hashing: "Проверка файла", upload: "Передача", uploading: "Передача", blob_upload: "Передача", stat: "Проверка результата", verify: "Проверка результата", verifying: "Проверка результата", commit: "Подтверждение", add: "Подтверждение", unlimited_add: "Подтверждение", preparing: "Подготовка" };
  let session = { authenticated: false, setup_required: false, csrf: "" };
  let sessionGeneration = 0;
  let quotaAutoRequested = false;
  let quotaAutoAccount = null;
  let snapshot = null;
  let activePage = "overview";
  let pollTimer = null;
  let pollInFlight = false;
  let toastTimer = null;
  let jobsFilter = "all";
  let jobsOffset = 0;
  let jobsTotal = 0;
  let jobsTotalBytes = null;
  let jobsSort = jobsPreferences.sort === "desc" ? "desc" : "asc";
  let jobsSortBy = Object.hasOwn(jobsSortFields, jobsPreferences.sort_by) ? jobsPreferences.sort_by : "created_at";
  let jobsLimit = jobsPageSizes.includes(jobsPreferences.limit) ? jobsPreferences.limit : 50;
  let actionMenuJobId = null;
  let actionMenuSignature = "";
  let deleteContext = null;
  let deletePreview = null;
  let deletePreviewRequest = 0;
  let deletePreviewLoading = false;
  let deletePreviewTimer = null;
  const warnedScans = (() => {
    try {
      const saved = JSON.parse(sessionStorage.getItem("mail-cloud.scan-warnings.v1") || "[]");
      return new Set(Array.isArray(saved) ? saved.filter((value) => typeof value === "string").slice(-100) : []);
    } catch { return new Set(); }
  })();
  let jobsRequest = 0;
  let schedules = [];
  let schedulesRequest = 0;
  let schedulesTimezone = "Europe/Moscow";
  let schedulesSignature = null;
  let editingScheduleId = null;
  let removingScheduleId = null;
  let folderState = { path: null, parent: null };
  let folderRequest = 0;
  let editingSourceId = null;
  let removingSourceId = null;
  let sourcesSignature = null;
  let settingsDirty = false;
  let connected = true;
  let accessBusy = false;
  let mailLogoutRequested = false;
  const pendingActions = new Set();
  const pendingTaskActions = new Map();
  const jobViews = new Map();

  function finite(value, fallback = 0) {
    return typeof value === "number" && Number.isFinite(value) ? value : fallback;
  }
  function count(value) { return numberFormat.format(Math.max(0, Math.floor(finite(value)))); }
  function taskCount(value) {
    const amount = Math.max(0, Math.floor(finite(value)));
    const lastTwo = amount % 100, last = amount % 10;
    const noun = lastTwo >= 11 && lastTwo <= 14 ? "задач" : last === 1 ? "задача" : last >= 2 && last <= 4 ? "задачи" : "задач";
    return count(amount) + " " + noun;
  }
  function fileCount(value) {
    const amount = Math.max(0, Math.floor(finite(value))), lastTwo = amount % 100, last = amount % 10;
    return count(amount) + " " + (lastTwo >= 11 && lastTwo <= 14 ? "файлов" : last === 1 ? "файл" : last >= 2 && last <= 4 ? "файла" : "файлов");
  }
  function bytes(value) {
    const amount = Math.max(0, finite(value));
    if (amount < 1024) return count(amount) + " Б";
    const units = ["КиБ", "МиБ", "ГиБ", "ТиБ", "ПиБ"];
    let scaled = amount / 1024, index = 0;
    while (scaled >= 1024 && index < units.length - 1) { scaled /= 1024; index += 1; }
    return scaled.toLocaleString("ru-RU", { maximumFractionDigits: scaled < 10 ? 2 : 1 }) + " " + units[index];
  }
  function dateTime(value) {
    if (!Number.isFinite(value) || value <= 0) return "";
    return new Date(value * 1000).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  function plain(value, fallback = "") { return typeof value === "string" ? value : fallback; }
  function mediaIcon(path) { return /\.(mp4|3gp|3g2|3gpp|3gpp2|avi|vob|mov|mkv|m2t|m2ts|ts|mts|f4v|flv|wmv|m4v|mpg|ogv|webm|hevc)$/i.test(path) ? "video" : "image"; }
  function setText(id, value) { const text = String(value); if ($(id).textContent !== text) $(id).textContent = text; }
  function setHidden(id, hidden) { $(id).hidden = Boolean(hidden); }
  function icon(name, extraClass = "") {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "icon" + (extraClass ? " " + extraClass : ""));
    svg.setAttribute("aria-hidden", "true");
    const use = document.createElementNS("http://www.w3.org/2000/svg", "use");
    use.setAttribute("href", "#i-" + name);
    svg.appendChild(use);
    return svg;
  }
  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  }
  function appendStorageMode(container, mode) {
    if (mode !== "regular") return;
    const label = element("span", "storage-mode", "В квоту");
    label.title = "Режим загрузки с расходованием места по тарифу";
    container.appendChild(label);
  }
  function setError(id, message = "") {
    setText(id, message);
    setHidden(id, !message);
  }
  function toast(message, error = false) {
    clearTimeout(toastTimer);
    setText("toast", message);
    $("toast").className = "toast" + (error ? " error" : "");
    setHidden("toast", false);
    toastTimer = setTimeout(() => setHidden("toast", true), error ? 7000 : 4200);
  }
  function showDialog(id) {
    const dialog = $(id);
    if (!dialog.open) dialog.showModal();
  }
  function closeDialogs() {
    document.querySelectorAll("dialog[open]").forEach((dialog) => dialog.close());
    $("mail-password").value = "";
    clearPanelPasswords();
  }

  async function api(path, options = {}) {
    const generation = sessionGeneration, passwordTransition = pendingActions.has("password");
    const method = options.method || "GET";
    const headers = { Accept: "application/json" };
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    const request = { method, headers, credentials: "same-origin", cache: "no-store", signal: controller.signal };
    if (method !== "GET") {
      headers["Content-Type"] = "application/json";
      headers["X-CSRF-Token"] = session.csrf || "";
      request.body = JSON.stringify(options.body || {});
    }
    let response;
    try {
      response = await fetch(path, request);
      let value;
      try { value = await response.json(); }
      catch { throw new Error("Сервер вернул неожиданный ответ. Повторите запрос."); }
      if (!response.ok) {
        const staleSession = generation !== sessionGeneration || passwordTransition || pendingActions.has("password");
        if (response.status === 401 && !path.startsWith("/api/session") && !staleSession) {
          session.authenticated = false;
          showAccess(false);
          setError("access-error", "Сессия панели завершилась. Войдите снова.");
        }
        const message = typeof value.error === "string" ? value.error : plain(value.error && value.error.message);
        const error = new Error(message || "Не удалось выполнить действие. Код ответа: " + response.status + ".");
        error.status = response.status;
        error.code = value.error && value.error.code;
        error.staleSession = staleSession;
        throw error;
      }
      return value;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("Сервер не ответил вовремя. Проверьте, что клиент запущен.");
      if (error instanceof TypeError) throw new Error("Нет связи с клиентом. Проверьте сервер или подключение.");
      throw error;
    } finally { clearTimeout(timeout); }
  }

  function showAccess(setup) {
    clearTimeout(pollTimer);
    closeDialogs();
    closeAccountMenu();
    closeTaskMenu();
    mailLogoutRequested = false;
    setHidden("loading-screen", true);
    setHidden("app", true);
    setHidden("access-screen", false);
    snapshot = null;
    renderConnectivity();
    session.setup_required = Boolean(setup);
    setText("access-title", setup ? "Защитите свою панель" : "Вход в панель");
    setText("access-description", setup ? "Придумайте пароль для управления фотоархивом. Он понадобится при открытии панели в новом браузере." : "Введите пароль этой панели, чтобы управлять загрузкой.");
    setText("access-password-label", setup ? "Новый пароль панели" : "Пароль панели");
    setText("access-password-hint", setup ? "Не менее 12 символов. Это отдельный пароль панели, не пароль Mail.ru." : "Это отдельный пароль панели управления, не пароль Mail.ru.");
    $("access-password").autocomplete = setup ? "new-password" : "current-password";
    $("access-password").minLength = setup ? 12 : 1;
    $("access-confirm").required = setup;
    setHidden("access-confirm-wrap", !setup);
    const submit = $("access-submit");
    submit.replaceChildren(document.createTextNode(setup ? "Создать пароль и войти" : "Войти"), icon("arrow"));
    submit.disabled = accessBusy;
  }

  async function refreshSession() {
    const value = await api("/api/session");
    session = { authenticated: value.authenticated === true, setup_required: value.setup_required === true, csrf: plain(value.csrf) };
    if (!session.authenticated) {
      showAccess(session.setup_required);
      return false;
    }
    setHidden("loading-screen", true);
    setHidden("access-screen", true);
    setHidden("app", false);
    return true;
  }

  function renderConnectivity() {
    let label = "Проверяем состояние…", status = "neutral", hint = "";
    if (!connected) {
      label = "Нет связи с сервером"; status = "offline";
    } else if (!session.authenticated) {
      label = "Войдите в панель";
    } else if (snapshot && snapshot.activity && snapshot.activity.phase === "login") {
      label = "Вход в Mail.ru…"; status = "checking";
    } else if (snapshot && snapshot.activity && snapshot.activity.phase === "logout") {
      label = "Выход из Mail.ru…"; status = "checking";
    } else if (snapshot && snapshot.authenticated === true) {
      label = "Mail.ru подключён"; status = "";
      hint = "Сессия Mail.ru сохранена. Доступ к Облаку проверяется при загрузке.";
    } else if (snapshot) {
      label = "Mail.ru не подключён";
    }
    const indicator = $("connection-status");
    indicator.className = "connection-status" + (status ? " " + status : "");
    indicator.lastElementChild.textContent = label;
    indicator.title = hint;
  }

  function setConnectivity(ok, message = "") {
    connected = ok;
    renderConnectivity();
    setHidden("connection-banner", ok);
    if (!ok) setText("connection-message", message || "Нет связи с клиентом. Последнее полученное состояние сохранено на экране.");
    if (snapshot) { renderApplicationStatus(snapshot); updateControls(); }
  }

  async function refreshState() {
    if (!session.authenticated) return;
    const generation = sessionGeneration;
    const value = await api("/api/state");
    if (generation !== sessionGeneration) return;
    const previouslyAuthenticated = snapshot && snapshot.authenticated;
    snapshot = value;
    setConnectivity(true);
    renderState();
    if (previouslyAuthenticated === false && value.authenticated === true) toast("Вход в Mail.ru сохранён.");
    if (mailLogoutRequested && value.authenticated === false && phase() !== "logout") {
      mailLogoutRequested = false;
      toast("Вы вышли из Mail.ru. Очередь сохранена.");
    } else if (phase() === "error") mailLogoutRequested = false;
  }

  async function poll() {
    if (document.hidden || pollInFlight || !session.authenticated || pendingActions.has("password")) return;
    pollInFlight = true;
    try {
      await refreshState();
      if (pendingActions.has("password")) return;
      if (activePage === "queue") await loadJobs(false);
      if (activePage === "schedules") await loadSchedules(false);
    } catch (error) {
      if (session.authenticated && !error.staleSession) setConnectivity(false, error.message + " Данные на экране могут быть устаревшими.");
    } finally {
      pollInFlight = false;
      clearTimeout(pollTimer);
      schedulePoll();
    }
  }

  function schedulePoll(soon = false) {
    clearTimeout(pollTimer);
    if (document.hidden || !session.authenticated || pendingActions.has("password")) return;
    pollTimer = setTimeout(poll, soon || isBusy() || anyAction() ? 2000 : 30000);
  }

  function phase() { return plain(snapshot && snapshot.activity && snapshot.activity.phase, "idle"); }
  function isBusy() { return busyPhases.has(phase()); }
  function anyAction() { return pendingActions.size > 0 || pendingTaskActions.size > 0; }
  function canControlTasks() { return Boolean(snapshot && snapshot.task_controls_available === true && connected && session.authenticated && pendingActions.size === 0); }
  function canClearQueue() { return Boolean(snapshot && snapshot.queue_clear_available === true && connected && session.authenticated && !anyAction()); }
  function canExportQueue() {
    return Boolean(snapshot && snapshot.configured === true && connected && session.authenticated
      && !pendingActions.has("password") && !["scanning", "reconciling"].includes(phase()));
  }

  function updateControls() {
    if (!snapshot) return;
    const hasSources = Array.isArray(snapshot.sources) && snapshot.sources.length > 0;
    const busy = isBusy();
    const transient = anyAction();
    const current = phase();
    const resumeDeletion = Boolean(snapshot.deletion && snapshot.deletion.resume_available === true);
    ["start-upload", "queue-start-upload"].forEach((id) => {
      $(id).disabled = busy || transient || !connected || !resumeDeletion && (!snapshot.authenticated || !hasSources);
    });
    ["scan-folders", "queue-scan-folders"].forEach((id) => { $(id).disabled = busy || transient || !hasSources || !connected; });
    const pausable = ["scanning", "checking", "uploading", "pausing", "reconciling", "deleting"].includes(current);
    ["pause-upload", "queue-pause-upload"].forEach((id) => {
      setHidden(id, !pausable);
      $(id).disabled = current === "pausing" || pendingActions.has("pause") || pendingActions.has("password") || !connected;
      $(id).querySelector("span").textContent = current === "scanning" ? "Остановить сканирование" : current === "pausing" ? "Сохраняем прогресс…" : current === "deleting" ? "Приостановить удаление" : "Приостановить";
    });
    const resumed = ["paused", "error"].includes(current) || finite(snapshot.stats && snapshot.stats.uploaded) > 0;
    ["start-upload", "queue-start-upload"].forEach((id) => {
      setHidden(id, pausable);
      $(id).querySelector("span").textContent = resumeDeletion ? "Продолжить удаление" : resumed ? "Продолжить загрузку" : "Начать загрузку";
    });
    $("export-queue").disabled = !canExportQueue();
    $("export-queue").title = ["scanning", "reconciling"].includes(current) ? "Дождитесь завершения сканирования" : "Скачать задачи выбранного фильтра";
    $("export-submit").disabled = !canExportQueue();
    const sourcesDisabled = busy || transient || resumeDeletion || !connected;
    ["add-source", "empty-add-source", "onboarding-source", "source-submit", "source-remove-submit", "browse-folders", "source-path", "source-alias", "source-remote-prefix"].forEach((id) => { $(id).disabled = sourcesDisabled; });
    document.querySelectorAll("[data-source-action]").forEach((button) => { button.disabled = sourcesDisabled; });
    setHidden("sources-busy-note", !busy && !resumeDeletion);
    setText("sources-busy-note", resumeDeletion ? "Завершите удаление, прежде чем изменять исходные папки." : "Чтобы изменить источники, приостановите текущую работу и дождитесь сохранения прогресса.");
    setHidden("source-form-busy", !busy);
    setHidden("source-remove-busy", !busy);
    ["onboarding-login", "account-menu-login"].forEach((id) => { $(id).disabled = busy || transient || !connected; });
    $("account-button").disabled = ["login", "logout"].includes(current) || transient || !connected;
    $("account-menu-logout").disabled = !snapshot.authenticated || current === "logout" || transient || !connected;
    setText("account-menu-note", busy ? "Выход остановит текущую работу и сохранит очередь." : "Очередь и завершённые задачи сохранятся. Для продолжения войдите в этот же аккаунт.");
    $("retry-errors").disabled = busy || transient || finite(snapshot.stats && snapshot.stats.failed) === 0 || !connected;
    $("save-settings").disabled = busy || transient || !connected;
    $("workers").disabled = busy || transient;
    $("auto-resume").disabled = busy || transient;
    $("regular-policy").disabled = busy || transient;
    $("unexpected-unlimited-action").disabled = busy || transient;
    $("remote-reconcile").disabled = busy || transient;
    $("timezone").disabled = busy || transient;
    $("change-panel-password").disabled = transient || !connected || !session.authenticated;
    const changingPassword = pendingActions.has("password");
    ["panel-password-current", "panel-password-new", "panel-password-confirm", "panel-password-submit"].forEach((id) => { $(id).disabled = changingPassword || !connected; });
    setText("panel-password-submit", changingPassword ? "Сохраняем…" : "Сменить пароль");
    ["logout-panel", "logout-panel-settings"].forEach((id) => { $(id).disabled = changingPassword; });
    const quotaRefreshing = snapshot.quota_refreshing === true || Boolean(snapshot.quota && snapshot.quota.refreshing) || pendingActions.has("quota");
    $("quota-refresh").disabled = snapshot.quota_refresh_available !== true || quotaRefreshing || transient || !connected;
    $("quota-refresh").querySelector("span").textContent = quotaRefreshing ? "Обновляем…" : "Обновить";
    const canClear = canClearQueue();
    $("clear-queue").disabled = !canClear;
    updateDeletionControls();
    setHidden("task-controls-note", snapshot.task_controls_available === true || !connected || jobsTotal === 0);
    jobViews.forEach(updateJobControls);
    updateTaskMenu();
    updateScheduleControls();
  }

  function renderApplicationStatus(state) {
    const stats = state.stats || {}, activity = state.activity || {}, scheduler = state.scheduler || {};
    const current = plain(activity.phase, "idle");
    const errors = finite(stats.failed), enabled = finite(scheduler.enabled);
    let title = "Готов к работе", description = "Все задачи обработаны. Можно запустить новое сканирование.", tone = "neutral", badge = "Ожидание", symbol = "cloud";
    if (!connected) {
      title = "Нет связи с сервером"; description = "На экране последние полученные данные. Проверяем подключение."; tone = "orange"; badge = "Нет связи"; symbol = "alert";
    } else if (["scanning", "reconciling"].includes(current)) {
      title = "Выполняется сканирование";
      description = plain(activity.message) || (current === "reconciling" ? "Сравниваем найденные файлы с актуальным содержимым Облака." : "Создаём новый список задач из выбранных исходных папок.");
      tone = "blue"; badge = current === "reconciling" ? "Сверка с Облаком" : "Сканирование"; symbol = "scan";
    } else if (current === "pausing") {
      title = "Приостанавливаем работу"; description = "Дожидаемся текущих операций и сохраняем результат."; tone = "orange"; badge = "Останавливаем"; symbol = "pause";
    } else if (current === "deleting") {
      title = "Выполняется удаление"; description = "Обрабатываем подтверждённый список задач и файлов."; tone = "orange"; badge = "Удаление"; symbol = "remove";
    } else if (finite(stats.running) > 0 || current === "uploading") {
      title = "Выполняется загрузка"; description = "Загружаются: " + taskCount(stats.running) + ". " + (enabled ? "Расписания включены." : "Ход выполнения показан в очереди."); tone = "blue"; badge = "Загрузка";
    } else if (["login", "logout", "checking", "quota"].includes(current)) {
      const info = phaseInfo[current] || phaseInfo.idle;
      title = info[0]; description = plain(activity.message) || "Дожидаемся ответа Облака."; tone = info[2]; badge = info[1]; symbol = info[3];
    } else if (state.auth_required === true || errors > 0 || plain(activity.last_error) || plain(scheduler.error)) {
      title = "Требуются действия";
      description = state.auth_required === true ? "Войдите в Mail.ru, чтобы продолжить работу." : errors > 0 ? "С ошибкой: " + taskCount(errors) + ". Подробности и повтор доступны в очереди." : plain(activity.last_error, plain(scheduler.error));
      tone = "orange"; badge = "Внимание"; symbol = "alert";
    } else if (state.deletion && state.deletion.resume_available === true) {
      title = "Требуются действия"; description = "Подтверждённое удаление приостановлено. Его можно продолжить."; tone = "orange"; badge = "На паузе"; symbol = "pause";
    } else if (!state.authenticated || !Array.isArray(state.sources) || state.sources.length === 0) {
      title = "Требуются действия";
      description = !state.authenticated ? "Подключите аккаунт Mail.ru для работы с Облаком." : "Добавьте исходные папки с фото и видео.";
      tone = "orange"; badge = "Настройка"; symbol = "alert";
    } else if (enabled > 0) {
      title = "Работа по расписанию";
      const next = scheduleDateTime(scheduler.next_run, plain(state.settings && state.settings.timezone, plain(scheduler.timezone, schedulesTimezone)));
      description = finite(scheduler.queued) > 0 ? "Сработавшее расписание ожидает запуска." : next ? "Следующий запуск: " + next + "." : "Включено расписаний: " + count(enabled) + ".";
      if (current === "paused") description = "Текущая работа на паузе. " + description;
      tone = "green"; badge = "Расписание"; symbol = "clock";
    } else if (current === "paused") {
      title = "Загрузка приостановлена"; description = "Можно продолжить всю очередь или отдельную задачу."; tone = "orange"; badge = "На паузе"; symbol = "pause";
    } else if (finite(stats.pending) + finite(stats.running) > 0) {
      description = "Ожидают запуска: " + taskCount(finite(stats.pending) + finite(stats.running)) + ".";
    } else if (finite(stats.total) === 0) {
      description = "Запустите сканирование, чтобы создать задачи для исходных папок.";
    }
    setText("transfer-title", title); setText("activity-message", description);
    setText("phase-badge", badge); $("phase-badge").className = "badge " + tone;
    $("phase-icon").dataset.tone = tone;
    const phaseUse = $("phase-icon").querySelector("use");
    if (phaseUse) phaseUse.setAttribute("href", "#i-" + symbol);
    setText("queue-activity-label", title);
  }

  function renderStorageCard(kind, plan) {
    const total = plan[kind];
    setText("stat-" + kind, total ? count(total.count) : "—");
    setText("stat-" + kind + "-bytes", total ? bytes(total.bytes) : "Объём неизвестен");
    ["unlimited", "regular", "unknown"].forEach((mode) => {
      const value = plan[kind + "_" + mode];
      setText("stat-" + kind + "-" + mode, value ? fileCount(value.count) + " · " + bytes(value.bytes) : "—");
      if (mode === "unknown") setHidden("stat-" + kind + "-unknown-row", !value || finite(value.count) === 0);
    });
  }

  function renderState() {
    const state = snapshot || {};
    renderConnectivity();
    const stats = state.stats || {};
    const sources = Array.isArray(state.sources) ? state.sources : [];
    const activity = state.activity || {};
    renderApplicationStatus(state);
    const current = phase();
    const total = Math.max(0, finite(stats.total));
    const uploaded = Math.max(0, finite(stats.uploaded));
    const failed = Math.max(0, finite(stats.failed)), skipped = Math.max(0, finite(stats.skipped));
    const existing = Math.min(skipped, Math.max(0, finite(stats.existing)));
    const finishedErrors = Math.max(0, finite(stats.permanent_failures ?? failed));
    const processed = Math.min(total, uploaded + finishedErrors + skipped);
    const percentage = total > 0 ? processed / total * 100 : 0;
    setText("version-label", "Версия " + plain(state.version, "—"));
    setText("account-label", state.authenticated ? plain(state.account, "Вход сохранён") : "Подключить Mail.ru");
    $("account-button").title = state.authenticated ? "Вход в Mail.ru сохранён: " + plain(state.account) : "Подключить аккаунт Mail.ru";
    $("account-button").setAttribute("aria-haspopup", "menu");
    setText("account-menu-email", plain(state.account, "Аккаунт Mail.ru"));
    setHidden("account-menu-login", state.authenticated === true);
    setHidden("account-menu-logout", state.authenticated !== true);
    setHidden("account-menu-note", state.authenticated !== true);
    setText("nav-queue-count", count(finite(stats.pending) + finite(stats.running)));
    setText("processed-count", count(processed));
    setText("total-count", count(total));
    setText("progress-percent", percentage.toLocaleString("ru-RU", { maximumFractionDigits: 1 }) + "%");
    $("overall-progress-fill").style.width = percentage + "%";
    $("overall-progress").setAttribute("aria-valuenow", percentage.toFixed(1));
    $("overall-progress").setAttribute("aria-valuetext", "Обработано задач: " + count(processed) + " из " + count(total));
    setText("transfer-bytes", "Загружено: " + bytes(state.storage_plan && state.storage_plan.uploaded && state.storage_plan.uploaded.bytes) + " · Объём задач: " + bytes(stats.total_bytes));
    setText("transfer-outcomes", "Успешно: " + count(uploaded) + (existing ? " · Уже в Облаке: " + count(existing) : "") + " · Ошибок: " + count(failed) + " · Пропущено: " + count(skipped - existing));
    setText("stat-pending", count(finite(stats.pending) + finite(stats.running)));
    const byStatus = stats.bytes_by_status || {};
    setText("stat-pending-bytes", bytes(finite(byStatus.pending) + finite(byStatus.running)));
    renderStorageCard("uploaded", state.storage_plan || {});
    renderStorageCard("existing", state.storage_plan || {});
    setText("stat-failed", count(failed));
    setText("stat-failed-bytes", bytes(byStatus.failed));
    setText("stat-sources", count(sources.length));
    setText("stat-sources-size", fileCount(total) + " · " + bytes(stats.total_bytes));
    const existingBytes = finite(state.storage_plan && state.storage_plan.existing && state.storage_plan.existing.bytes);
    setText("stat-skipped", "Пропущено: " + count(skipped - existing) + " · " + bytes(Math.max(0, finite(byStatus.skipped) - existingBytes)));
    const scan = activity.scan || {};
    setHidden("scan-live", current !== "scanning");
    setText("scan-seen", count(scan.seen));
    setText("scan-added", count(scan.added));
    setText("scan-changed", count(scan.changed));
    const reconcile = activity.reconcile || {};
    setHidden("reconcile-live", current !== "reconciling" || !(state.settings && state.settings.remote_reconcile === true));
    setText("reconcile-checked", count(reconcile.checked));
    setText("reconcile-matched", count(reconcile.matched));
    setText("reconcile-errors", count(reconcile.errors));
    setText("reconcile-total", typeof reconcile.total === "number" && Number.isFinite(reconcile.total) && reconcile.total >= 0 ? " из " + count(reconcile.total) : "");
    const error = plain(activity.last_error);
    setHidden("activity-error", !error);
    setText("activity-error-text", error);
    const lastScan = state.last_scan;
    setText("last-scan-label", lastScan && typeof lastScan === "object" ? (lastScan.complete === true ? "Последний обход: " : "Обход не завершён: ") + count(lastScan.seen) + " файлов" : "Сканирование ещё не выполнялось");
    renderOnboarding(state, sources);
    renderActiveFiles(Array.isArray(activity.current_files) ? activity.current_files : []);
    renderEvents(Array.isArray(state.events) ? state.events : []);
    renderSources(sources);
    renderSettings(state);
    renderQuota(state);
    renderStoragePlan(state);
    renderDeletionProgress(state);
    schedulesTimezone = plain(state.settings && state.settings.timezone, plain(state.scheduler && state.scheduler.timezone, schedulesTimezone));
    setText("scheduler-timezone", "Часовой пояс: " + schedulesTimezone);
    const schedulerError = plain(state.scheduler && state.scheduler.error);
    setText("scheduler-error", schedulerError); setHidden("scheduler-error", !schedulerError);
    setHidden("scheduled-pause-note", current !== "paused" || finite(state.scheduler && state.scheduler.enabled) === 0);
    if (activePage === "schedules") renderSchedules();
    updateControls();
    maybeShowScanWarning();
    maybeRefreshQuota();
  }

  function renderQuota(state) {
    const quota = state.quota && typeof state.quota === "object" ? state.quota : null;
    const refreshing = state.quota_refreshing === true || Boolean(quota && quota.refreshing);
    const valid = Boolean(state.authenticated && quota && typeof quota.used_bytes === "number" && Number.isFinite(quota.used_bytes) && quota.used_bytes >= 0 && typeof quota.total_bytes === "number" && Number.isFinite(quota.total_bytes) && quota.total_bytes >= 0);
    setHidden("quota-content", !valid); setHidden("quota-empty", valid);
    setHidden("account-quota", !valid);
    setHidden("account-quota-empty", valid || !state.authenticated);
    setText("account-quota-empty", refreshing ? "Получаем данные о месте…" : "Данные о месте ещё не получены.");
    setText("quota-empty", !state.authenticated ? "Подключите Mail.ru, чтобы получить данные о свободном месте." : refreshing ? "Получаем данные о месте в аккаунте…" : "Данные о квоте ещё не получены. Нажмите «Обновить», когда кнопка станет доступна.");
    if (valid) {
      const used = quota.used_bytes, total = quota.total_bytes;
      const percent = total > 0 ? Math.max(0, Math.min(100, used / total * 100)) : used > 0 ? 100 : 0;
      setText("quota-used", bytes(used) + " занято из " + bytes(total));
      setText("quota-free", typeof quota.free_bytes === "number" && Number.isFinite(quota.free_bytes) ? "Свободно: " + bytes(quota.free_bytes) : "");
      setText("account-quota-used", bytes(used) + " из " + bytes(total));
      setText("account-quota-free", typeof quota.free_bytes === "number" && Number.isFinite(quota.free_bytes) ? "Свободно " + bytes(quota.free_bytes) : "Свободное место неизвестно");
      $("account-quota-fill").style.width = percent + "%";
      $("account-quota-progress").setAttribute("aria-valuenow", percent.toFixed(1));
      $("account-quota-progress").setAttribute("aria-valuetext", bytes(used) + " занято из " + bytes(total));
      setText("account-quota-note", quota.stale ? "Последние полученные данные" : quota.updated_at ? "Обновлено: " + dateTime(quota.updated_at) : "");
      $("quota-progress-fill").style.width = percent + "%";
      $("quota-progress").setAttribute("aria-valuenow", percent.toFixed(1));
      $("quota-progress").setAttribute("aria-valuetext", bytes(used) + " занято из " + bytes(total));
      const breakdown = [];
      if (typeof quota.cloud_bytes === "number" && Number.isFinite(quota.cloud_bytes)) breakdown.push("Облако: " + bytes(quota.cloud_bytes));
      if (typeof quota.mail_bytes === "number" && Number.isFinite(quota.mail_bytes)) breakdown.push("Почта: " + bytes(quota.mail_bytes));
      setText("quota-breakdown", breakdown.join(" · "));
      setText("quota-updated", quota.updated_at ? "Обновлено: " + dateTime(quota.updated_at) : "");
    }
    const error = state.authenticated ? plain(quota && quota.error, plain(state.quota_error)) : "";
    const note = error ? "Не удалось обновить данные: " + error : refreshing && valid ? "Обновляем данные…" : quota && quota.stale && valid ? "Показаны последние сохранённые данные. Их можно обновить кнопкой выше." : "";
    setText("quota-note", note); setHidden("quota-note", !note);
    $("quota-note").className = "quota-note" + (error ? " error" : "");
  }

  function maybeRefreshQuota() {
    if (!snapshot || !snapshot.authenticated) { quotaAutoRequested = false; quotaAutoAccount = null; return; }
    const account = plain(snapshot.account);
    if (account !== quotaAutoAccount) { quotaAutoAccount = account; quotaAutoRequested = false; }
    if (quotaAutoRequested || snapshot.quota !== null || snapshot.quota_refresh_available !== true || snapshot.quota_refreshing === true || isBusy() || anyAction() || !connected) return;
    quotaAutoRequested = true;
    perform("quota", "/api/quota/refresh");
  }

  function renderOnboarding(state, sources) {
    const completed = [state.authenticated === true, sources.length > 0];
    setHidden("onboarding", completed.every(Boolean));
    setText("setup-count", completed.filter(Boolean).length + " из 2");
    ["account", "sources"].forEach((name, index) => {
      const node = $("step-" + name);
      node.classList.toggle("done", completed[index]);
      const number = node.querySelector(".step-number");
      if (completed[index]) { number.replaceChildren(icon("check")); number.firstChild.style.width = "13px"; number.firstChild.style.height = "13px"; }
      else number.textContent = String(index + 1);
      node.querySelector("button").hidden = completed[index];
    });
    setText("step-account-text", completed[0] ? plain(state.account, "Вход сохранён") : "Войдите с паролем приложения");
    setText("step-sources-text", completed[1] ? "Подключено папок: " + count(sources.length) : "Откуда читать и куда загружать файлы");
  }

  function renderStoragePlan(state) {
    const plan = state.storage_plan || {};
    ["unlimited", "regular", "uploaded"].forEach((kind) => {
      const value = plan[kind];
      setText("plan-" + kind + "-bytes", value ? bytes(value.bytes) : "—");
      setText("plan-" + kind + "-count", value ? taskCount(value.count) : "Данные ещё не получены");
    });
    setText("plan-policy", state.settings && state.settings.regular_policy === "upload" ? "Файлы с расходом квоты будут загружены в обычное хранилище." : "Файлы с расходом квоты будут пропущены.");
    const detail = [];
    if (plan.uploaded_unlimited) detail.push("В безлимите: " + bytes(plan.uploaded_unlimited.bytes));
    if (plan.uploaded_regular) detail.push("В квоте: " + bytes(plan.uploaded_regular.bytes));
    if (plan.uploaded_unknown && plan.uploaded_unknown.count) detail.push("Режим не определён: " + bytes(plan.uploaded_unknown.bytes));
    setText("plan-uploaded-detail", detail.join(" · "));
    if ($("scan-warning-dialog").open) {
      if (scanWarningEligible()) renderScanWarning();
      else $("scan-warning-dialog").close();
    }
  }

  function renderScanWarning() {
    const regular = snapshot && snapshot.storage_plan && snapshot.storage_plan.regular || {};
    setText("scan-warning-count", taskCount(regular.count));
    setText("scan-warning-bytes", bytes(regular.bytes));
    const quota = snapshot && snapshot.quota;
    const known = Boolean(snapshot && snapshot.authenticated && quota && typeof quota.free_bytes === "number" && Number.isFinite(quota.free_bytes) && quota.free_bytes >= 0);
    const upload = snapshot && snapshot.settings && snapshot.settings.regular_policy === "upload";
    let quotaText = known ? "Свободно в аккаунте: " + bytes(quota.free_bytes) + (quota.stale ? " (последние полученные данные)." : ".") : "Свободное место пока неизвестно: данные о квоте ещё не получены.";
    if (known && finite(regular.bytes) > quota.free_bytes) quotaText += " Для всех этих файлов места недостаточно.";
    setText("scan-warning-quota", quotaText);
    setText("scan-warning-policy", upload ? "По вашим настройкам эти файлы будут загружены и займут место в квоте." : "По вашим настройкам эти файлы будут пропущены. Разрешить их загрузку можно в настройках.");
    $("scan-warning-policy").className = "notice " + (upload ? "warning" : "info");
  }

  function scanWarningEligible() {
    const scan = snapshot && snapshot.last_scan, regular = snapshot && snapshot.storage_plan && snapshot.storage_plan.regular;
    return Boolean(scan && scan.complete === true && scan.trigger === "manual" && scan.warning_allowed === true
      && (snapshot.scan_id ?? scan.scan_id) != null && !["scanning", "reconciling"].includes(phase())
      && regular && finite(regular.count) > 0);
  }

  function maybeShowScanWarning() {
    if (!snapshot || !session.authenticated || document.hidden || document.querySelector("dialog[open]") || !scanWarningEligible()) return;
    const scan = snapshot.last_scan;
    const id = snapshot.scan_id ?? scan.scan_id;
    const key = plain(snapshot.account) + ":" + String(id);
    if (warnedScans.has(key)) return;
    warnedScans.add(key);
    while (warnedScans.size > 100) warnedScans.delete(warnedScans.values().next().value);
    try { sessionStorage.setItem("mail-cloud.scan-warnings.v1", JSON.stringify([...warnedScans])); } catch { /* Memory deduplication still applies when browser storage is unavailable. */ }
    renderScanWarning(); showDialog("scan-warning-dialog");
  }

  function renderDeletionProgress(state) {
    if (phase() !== "deleting") return;
    const progress = state.deletion || state.activity && state.activity.deletion;
    if (!progress || typeof progress !== "object") return;
    const done = progress.processed ?? progress.done;
    const total = progress.total ?? progress.task_count;
    if (Number.isFinite(done) && Number.isFinite(total)) setText("activity-message", "Обработано " + count(done) + " из " + count(total) + " задач. Результат удаления сохраняется.");
  }

  function emptyBlock(symbol, title, description, compact = false) {
    const empty = element("div", "empty-state" + (compact ? " compact-empty" : ""));
    const mark = element("span", "empty-symbol"); mark.appendChild(icon(symbol));
    empty.append(mark, element("strong", "", title), element("p", "", description));
    return empty;
  }

  function renderActiveFiles(files) {
    const list = $("active-files");
    list.replaceChildren();
    setText("active-count", taskCount(files.length));
    if (!files.length) {
      let title = "Активных задач пока нет", message = "Здесь появятся задачи, которые выполняются прямо сейчас.";
      if (phase() === "paused") { title = "Очередь на паузе"; message = "Нажмите «Продолжить загрузку», когда будете готовы."; }
      if (phase() === "completed") { title = "Все текущие задачи обработаны"; message = "После изменения файлов на сервере нажмите «Пересканировать папки»."; }
      if (phase() === "scanning") { title = "Собираем список файлов"; message = "Сканирование не копирует файлы и не создаёт миниатюры."; }
      list.appendChild(emptyBlock("image", title, message, true));
      return;
    }
    files.slice(0, 16).forEach((file) => {
      const fullPath = plain(file.path, "Файл");
      const name = fullPath.split(/[\\/]/).filter(Boolean).pop() || fullPath;
      const size = Math.max(0, finite(file.size)), sent = Math.max(0, finite(file.sent));
      const percent = size > 0 ? Math.min(100, sent / size * 100) : 0;
      const stage = plain(file.stage);
      const item = element("div", "active-file");
      const heading = element("div", "active-file-heading");
      const fileIcon = element("span", "file-icon"); fileIcon.appendChild(icon(mediaIcon(name)));
      const label = element("div");
      const title = element("div", "active-file-name", name); title.title = fullPath;
      const path = element("div", "active-file-path", fullPath); path.title = fullPath;
      label.append(title, path);
      appendStorageMode(label, file.storage_mode);
      heading.append(fileIcon, label, element("span", "badge blue", transferStages[stage] || "Обработка"));
      const progress = element("div", "active-file-progress");
      const track = element("div", "progress-track"); track.setAttribute("role", "progressbar"); track.setAttribute("aria-label", name); track.setAttribute("aria-valuemin", "0"); track.setAttribute("aria-valuemax", "100"); track.setAttribute("aria-valuenow", percent.toFixed(1));
      const fill = element("div", "progress-fill"); fill.style.width = percent + "%"; track.appendChild(fill);
      progress.append(track, element("span", "", Math.floor(percent) + "%"));
      item.append(heading, progress, element("div", "active-file-size", bytes(Math.min(sent, size || sent)) + " / " + bytes(size)));
      list.appendChild(item);
    });
  }

  function renderEvents(events) {
    const container = $("events-list");
    container.replaceChildren();
    if (!events.length) { container.appendChild(element("p", "events-empty", "События появятся после начала работы.")); return; }
    events.slice().sort((a, b) => finite(b.at) - finite(a.at)).slice(0, 12).forEach((entry) => {
      const row = element("div", "event-row");
      const level = ["error", "warning", "success"].includes(entry.level) ? entry.level : "";
      const body = element("div", "event-body");
      body.append(element("div", "event-message", plain(entry.message).slice(0, 800)), element("div", "event-time", dateTime(entry.at)));
      row.append(element("span", "event-dot " + level), body);
      container.appendChild(row);
    });
  }

  function renderSources(sources) {
    setHidden("sources-empty", sources.length > 0);
    const signature = JSON.stringify(sources.map((source) => [source.id, source.path, source.alias, source.remote_prefix]));
    if (signature === sourcesSignature) return;
    sourcesSignature = signature;
    const list = $("sources-list"); list.replaceChildren();
    sources.forEach((source) => {
      const card = element("article", "card source-card");
      const symbol = element("span", "stat-icon blue"); symbol.appendChild(icon("folder"));
      const info = element("div", "source-info");
      const heading = element("div", "source-title-line");
      heading.append(element("h2", "", plain(source.alias, "Папка")), element("span", "badge neutral", "Все подпапки"));
      info.appendChild(heading);
      const local = element("div", "source-path-line"), remote = element("div", "source-path-line");
      local.append(element("span", "", "На сервере"), element("span", "", plain(source.path)));
      remote.append(element("span", "", "В Облаке"), element("span", "", plain(source.remote_prefix)));
      info.append(local, remote);
      const actions = element("div", "source-actions");
      const edit = element("button", "button compact"); edit.type = "button";
      edit.dataset.sourceAction = "edit"; edit.dataset.sourceId = String(source.id);
      edit.setAttribute("aria-label", "Изменить источник «" + plain(source.alias, "Папка") + "»");
      edit.append(icon("edit", "small"), document.createTextNode("Изменить"));
      edit.addEventListener("click", () => openSource(source.id));
      const remove = element("button", "button compact danger subtle"); remove.type = "button";
      remove.dataset.sourceAction = "remove"; remove.dataset.sourceId = String(source.id);
      remove.setAttribute("aria-label", "Убрать источник «" + plain(source.alias, "Папка") + "»");
      remove.append(icon("remove", "small"), document.createTextNode("Убрать"));
      remove.addEventListener("click", () => openSourceRemoval(source.id));
      actions.append(edit, remove);
      card.append(symbol, info, actions);
      list.appendChild(card);
    });
  }

  function renderSettings(state) {
    if (!settingsDirty && !pendingActions.has("settings")) {
      $("workers").value = String(finite(state.settings && state.settings.workers, 2));
      $("auto-resume").checked = !state.settings || state.settings.auto_resume !== false;
      $("regular-policy").value = state.settings && state.settings.regular_policy === "upload" ? "upload" : "skip";
      $("unexpected-unlimited-action").value = state.settings && state.settings.unexpected_unlimited_action === "keep" ? "keep" : "trash";
      $("remote-reconcile").checked = Boolean(state.settings && state.settings.remote_reconcile === true);
      $("timezone").value = plain(state.settings && state.settings.timezone, "Europe/Moscow");
    }
  }

  function navigate(page) {
    closeTaskMenu();
    activePage = Object.prototype.hasOwnProperty.call(pageNames, page) ? page : "overview";
    Object.keys(pageNames).forEach((name) => setHidden("page-" + name, name !== activePage));
    document.querySelectorAll(".nav-item").forEach((item) => {
      const active = item.dataset.page === activePage;
      item.classList.toggle("active", active);
      if (active) item.setAttribute("aria-current", "page"); else item.removeAttribute("aria-current");
    });
    setText("breadcrumb-page", pageNames[activePage]);
    document.title = pageNames[activePage] + " · Фотоархив";
    if (activePage === "queue" && session.authenticated) loadJobs();
    if (activePage === "schedules" && session.authenticated) loadSchedules();
  }

  function renderJobsSort() {
    $("jobs-limit").value = String(jobsLimit);
    document.querySelectorAll("[data-sort-by]").forEach((button) => {
      const active = button.dataset.sortBy === jobsSortBy;
      button.parentElement.setAttribute("aria-sort", active ? jobsSort === "asc" ? "ascending" : "descending" : "none");
      button.setAttribute("aria-label", jobsSortFields[button.dataset.sortBy] + ": сортировать " + (active && jobsSort === "asc" ? "по убыванию" : "по возрастанию"));
      button.title = active ? jobsSort === "asc" ? "По возрастанию. Нажмите, чтобы изменить порядок." : "По убыванию. Нажмите, чтобы изменить порядок." : "Сортировать по этому столбцу";
      button.classList.toggle("active", active);
      button.querySelector("use").setAttribute("href", active ? "#i-chevron" : "#i-sort");
    });
  }

  function saveJobsPreferences() {
    try { localStorage.setItem("mail-cloud.queue.v2", JSON.stringify({ sort_by: jobsSortBy, sort: jobsSort, limit: jobsLimit })); }
    catch { /* In-memory preferences still apply when browser storage is unavailable. */ }
  }

  async function loadJobs(showLoading = true) {
    if (!session.authenticated || pendingActions.has("password")) return;
    const request = ++jobsRequest, generation = sessionGeneration;
    const offset = jobsOffset, filter = jobsFilter, sort = jobsSort, sortBy = jobsSortBy, limit = jobsLimit;
    renderJobsSort();
    if (showLoading) setText("jobs-loading", "Обновляем…");
    try {
      const query = new URLSearchParams({ status: filter, offset: String(offset), limit: String(limit), sort, sort_by: sortBy });
      const result = await api("/api/jobs?" + query.toString());
      if (request !== jobsRequest || generation !== sessionGeneration || offset !== jobsOffset || filter !== jobsFilter
          || sort !== jobsSort || sortBy !== jobsSortBy || limit !== jobsLimit) return;
      jobsTotal = Math.max(0, finite(result.total));
      jobsTotalBytes = typeof result.total_bytes === "number" && Number.isFinite(result.total_bytes) ? Math.max(0, result.total_bytes) : null;
      if (jobsOffset >= jobsTotal && jobsOffset > 0) {
        jobsOffset = Math.max(0, Math.floor(Math.max(0, jobsTotal - 1) / jobsLimit) * jobsLimit);
        return loadJobs(showLoading);
      }
      renderJobs(Array.isArray(result.jobs) ? result.jobs : []);
      setText("jobs-loading", "");
    } catch (error) {
      if (request === jobsRequest && !error.staleSession) setText("jobs-loading", error.message);
    }
  }

  function scheduleDateTime(value, timezone = schedulesTimezone) {
    if (!Number.isFinite(value) || value <= 0) return "";
    const zone = timezone || snapshot && snapshot.settings && snapshot.settings.timezone || "Europe/Moscow";
    try {
      return new Intl.DateTimeFormat("ru-RU", { timeZone: zone, day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" }).format(new Date(value * 1000));
    } catch { return dateTime(value); }
  }

  function scheduleById(id) { return schedules.find((value) => value.id === id); }
  function canManageSchedules() {
    return Boolean(snapshot && connected && session.authenticated && !pendingActions.has("password") && !pendingActions.has("schedule"));
  }
  function updateScheduleControls() {
    const available = canManageSchedules(), saving = pendingActions.has("schedule");
    ["add-schedule", "empty-add-schedule"].forEach((id) => { $(id).disabled = !available; });
    document.querySelectorAll("[data-schedule-action]").forEach((button) => { button.disabled = !available; });
    $("schedule-submit").disabled = !available || !snapshot || !Array.isArray(snapshot.sources) || snapshot.sources.length === 0;
    $("schedule-remove-submit").disabled = !available;
    setText("schedule-submit", saving ? "Сохраняем…" : "Сохранить расписание");
    setText("schedule-remove-submit", saving ? "Удаляем…" : "Удалить расписание");
    $("schedule-form").querySelectorAll("input,fieldset,button").forEach((input) => { if (input.id !== "schedule-submit") input.disabled = saving; });
    $("schedule-remove-dialog").querySelectorAll("[data-close-dialog]").forEach((button) => { button.disabled = saving; });
  }

  async function loadSchedules(showLoading = true) {
    if (!session.authenticated || pendingActions.has("password") || pendingActions.has("schedule")) return;
    const request = ++schedulesRequest, generation = sessionGeneration;
    if (showLoading) setText("schedules-loading", "Обновляем…");
    try {
      const result = await api("/api/schedules");
      if (request !== schedulesRequest || generation !== sessionGeneration) return;
      applySchedules(result); setText("schedules-loading", "");
    } catch (error) {
      if (request === schedulesRequest && !error.staleSession) setText("schedules-loading", error.message);
    }
  }

  function applySchedules(result) {
    schedules = Array.isArray(result.schedules) ? result.schedules : [];
    schedulesTimezone = plain(result.timezone, snapshot && snapshot.settings && snapshot.settings.timezone || "Europe/Moscow");
    renderSchedules(); updateScheduleControls();
  }

  function renderSchedules() {
    const sources = snapshot && Array.isArray(snapshot.sources) ? snapshot.sources : [];
    setText("scheduler-timezone", "Часовой пояс: " + schedulesTimezone);
    const signature = JSON.stringify([schedules, schedulesTimezone, sources.map((source) => [source.id, source.alias, source.path])]);
    if (signature === schedulesSignature) return;
    schedulesSignature = signature;
    const focused = document.activeElement && document.activeElement.dataset.scheduleAction ?
      { id: document.activeElement.dataset.scheduleId, action: document.activeElement.dataset.scheduleAction } : null;
    const list = $("schedules-list"); list.replaceChildren();
    setHidden("schedules-empty", schedules.length > 0);
    const weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"];
    const runStates = { complete: ["Завершено", "green"], completed: ["Завершено", "green"], error: ["Ошибка", "red"], paused: ["На паузе", "orange"], running: ["В работе", "blue"], queued: ["Ожидает очередь", "orange"], cancelled: ["Отменено", "neutral"] };
    schedules.forEach((schedule) => {
      const card = element("article", "card schedule-card" + (schedule.enabled ? "" : " is-disabled"));
      const clock = element("div", "schedule-clock");
      clock.append(element("strong", "", plain(schedule.time, "—")));
      const days = schedule.mode === "monthly" ? "Числа: " + (Array.isArray(schedule.month_days) ? schedule.month_days.join(", ") : "—") :
        Array.isArray(schedule.weekdays) && schedule.weekdays.length === 7 ? "Каждый день" :
          (Array.isArray(schedule.weekdays) ? schedule.weekdays.map((value) => weekdays[value - 1]).filter(Boolean).join(", ") : "—");
      const repeat = element("span", "schedule-repeat-label", days); repeat.title = days; clock.appendChild(repeat);
      const info = element("div", "schedule-info"), title = element("h2", "", plain(schedule.name, "Расписание"));
      const selected = Array.isArray(schedule.source_ids) ? schedule.source_ids : [];
      const sourceNames = selected.map((id) => { const source = sources.find((value) => value.id === id); return source ? plain(source.alias, source.path) : "Источник недоступен"; });
      const sourceLine = element("p", "schedule-source-summary", sourceNames.length ? sourceNames.join(" · ") : "Исходные папки не выбраны");
      sourceLine.title = sourceNames.join("\n"); info.append(title, sourceLine);
      const next = element("p", "schedule-next", schedule.queued ? "Ожидает завершения текущей операции" : !schedule.enabled ? "Новые запуски выключены" : schedule.next_run ? "Следующий запуск: " + scheduleDateTime(schedule.next_run) : "Нет доступной даты запуска");
      info.appendChild(next);
      const last = schedule.last_run;
      if (last && typeof last === "object") {
        const status = runStates[plain(last.status)] || ["Последний запуск", "neutral"];
        const history = element("div", "schedule-last-run");
        history.append(element("span", "badge " + status[1], status[0]));
        const at = scheduleDateTime(last.finished_at || last.started_at);
        if (at) history.appendChild(element("span", "", at));
        if (last.message) {
          const message = element("span", "schedule-last-message", plain(last.message)); message.title = plain(last.message); history.appendChild(message);
        }
        info.appendChild(history);
      } else if (schedule.queued) info.appendChild(element("span", "badge orange", "Ожидает очередь"));
      const actions = element("div", "schedule-card-actions");
      const enabled = element("label", "schedule-switch");
      const toggle = element("input"); toggle.type = "checkbox"; toggle.checked = schedule.enabled === true;
      toggle.setAttribute("role", "switch"); toggle.setAttribute("aria-label", "Включить расписание «" + plain(schedule.name) + "»");
      toggle.dataset.scheduleId = String(schedule.id); toggle.dataset.scheduleAction = "toggle";
      enabled.append(toggle, element("span", "switch"));
      toggle.addEventListener("change", async () => {
        const wanted = toggle.checked;
        const saved = await mutateSchedule("/api/schedules/toggle", { id: schedule.id, enabled: wanted }, { message: wanted ? "Расписание включено." : "Расписание выключено." });
        if (!saved && toggle.isConnected) toggle.checked = schedule.enabled === true;
      });
      const edit = element("button", "icon-button"), remove = element("button", "icon-button schedule-remove");
      edit.type = remove.type = "button";
      edit.appendChild(icon("edit", "small")); remove.appendChild(icon("remove", "small"));
      edit.setAttribute("aria-label", "Изменить расписание «" + plain(schedule.name) + "»"); edit.title = "Изменить";
      remove.setAttribute("aria-label", "Удалить расписание «" + plain(schedule.name) + "»"); remove.title = "Удалить";
      [edit, remove].forEach((button) => { button.dataset.scheduleId = String(schedule.id); });
      edit.dataset.scheduleAction = "edit"; remove.dataset.scheduleAction = "remove";
      edit.addEventListener("click", () => openSchedule(schedule.id));
      remove.addEventListener("click", () => openScheduleRemoval(schedule.id));
      actions.append(enabled, edit, remove); card.append(clock, info, actions); list.appendChild(card);
    });
    if (focused && !document.querySelector("dialog[open]")) {
      const target = [...document.querySelectorAll("[data-schedule-action]")].find((node) => node.dataset.scheduleId === focused.id && node.dataset.scheduleAction === focused.action);
      (target || $("schedules-title")).focus({ preventScroll: true });
    }
  }

  function updateScheduleMode() {
    const monthly = $("schedule-monthly").checked;
    setHidden("schedule-month-days-wrap", !monthly); setHidden("schedule-weekdays", monthly);
    $("schedule-month-days").required = monthly;
  }
  function openSchedule(id = null) {
    if (!canManageSchedules()) return;
    const value = id === null ? null : scheduleById(id);
    if (id !== null && !value) { toast("Это расписание уже удалено."); return; }
    editingScheduleId = id;
    $("schedule-form").reset(); setError("schedule-error");
    setText("schedule-dialog-title", value ? "Изменить расписание" : "Добавить расписание");
    $("schedule-name").value = value ? plain(value.name) : "";
    $("schedule-time").value = value ? plain(value.time, "03:00") : "03:00";
    $("schedule-enabled").checked = !value || value.enabled === true;
    $("schedule-monthly").checked = Boolean(value && value.mode === "monthly");
    $("schedule-weekly").checked = !$("schedule-monthly").checked;
    $("schedule-month-days").value = value && Array.isArray(value.month_days) ? value.month_days.join(", ") : "";
    const weekdays = value && Array.isArray(value.weekdays) ? value.weekdays : [1, 2, 3, 4, 5, 6, 7];
    document.querySelectorAll('[name="schedule-weekday"]').forEach((input) => { input.checked = weekdays.includes(Number(input.value)); });
    setText("schedule-timezone-hint", "Часовой пояс: " + schedulesTimezone);
    const selected = value && Array.isArray(value.source_ids) ? value.source_ids : [];
    const sourceList = $("schedule-source-list"); sourceList.replaceChildren();
    const sources = snapshot && Array.isArray(snapshot.sources) ? snapshot.sources : [];
    sources.forEach((source) => {
      const label = element("label", "checkbox-row schedule-source-choice"), checkbox = element("input"), body = element("span");
      checkbox.type = "checkbox"; checkbox.name = "schedule-source"; checkbox.value = String(source.id); checkbox.checked = selected.includes(source.id);
      body.append(element("strong", "", plain(source.alias, "Источник")), element("small", "", plain(source.path)));
      label.append(checkbox, body); sourceList.appendChild(label);
    });
    setHidden("schedule-no-sources", sources.length > 0);
    updateScheduleMode(); updateScheduleControls(); showDialog("schedule-dialog");
  }
  function openScheduleRemoval(id) {
    if (!canManageSchedules()) return;
    const value = scheduleById(id); if (!value) return;
    removingScheduleId = id; setText("schedule-remove-name", plain(value.name)); setError("schedule-remove-error");
    updateScheduleControls(); showDialog("schedule-remove-dialog");
  }

  async function mutateSchedule(path, body, options = {}) {
    if (!canManageSchedules()) return false;
    pendingActions.add("schedule"); schedulesRequest += 1; updateControls();
    if (options.errorId) setError(options.errorId);
    try {
      const result = await api(path, { method: "POST", body });
      applySchedules(result);
      if (options.close) $(options.close).close();
      await refreshState();
      if (options.message) toast(options.message);
      return true;
    } catch (error) {
      if (options.errorId) setError(options.errorId, error.message); else toast(error.message, true);
      return false;
    } finally { pendingActions.delete("schedule"); updateControls(); schedulePoll(true); }
  }

  async function saveSchedule(event) {
    event.preventDefault(); if (!canManageSchedules()) return;
    const sourceIds = [...document.querySelectorAll('[name="schedule-source"]:checked')].map((input) => Number(input.value));
    if (!sourceIds.length) { setError("schedule-error", "Выберите хотя бы одну исходную папку."); return; }
    const mode = $("schedule-monthly").checked ? "monthly" : "weekly";
    const weekdays = [...document.querySelectorAll('[name="schedule-weekday"]:checked')].map((input) => Number(input.value));
    let monthDays = [];
    if (mode === "weekly" && !weekdays.length) { setError("schedule-error", "Выберите хотя бы один день недели."); return; }
    if (mode === "monthly") {
      const pieces = $("schedule-month-days").value.split(",").map((value) => value.trim());
      if (!pieces.length || pieces.some((value) => !/^\d{1,2}$/.test(value) || Number(value) < 1 || Number(value) > 30)) {
        setError("schedule-error", "Введите числа от 1 до 30 через запятую, например 1, 15, 30."); $("schedule-month-days").focus(); return;
      }
      monthDays = [...new Set(pieces.map(Number))].sort((a, b) => a - b);
    }
    const body = { name: $("schedule-name").value.trim(), source_ids: sourceIds, time: $("schedule-time").value,
      mode, weekdays, month_days: monthDays, enabled: $("schedule-enabled").checked };
    if (editingScheduleId !== null) body.id = editingScheduleId;
    await mutateSchedule("/api/schedules/save", body, { errorId: "schedule-error", close: "schedule-dialog", message: "Расписание сохранено." });
  }

  function taskActions(job) {
    if (job.remove_requested === true || job.status === "uploaded" || job.status === "skipped" && job.skip_reason === "remote_exists") return [];
    if (job.status === "running") return job.paused === true ? [] : [["pause", "Остановить", "pause"]];
    if (job.status === "skipped") return [["return", "Вернуть в очередь", "retry"]];
    if (job.status === "failed") return [["start", "Повторить попытку", "retry"], ["skip", "Пропустить", "skip"]];
    if (job.paused === true || ["pending", "paused"].includes(job.status)) return [["start", job.paused === true ? "Продолжить" : "Запустить", "play"], ["skip", "Пропустить", "skip"]];
    return [];
  }

  function taskActionDisabledReason(job, action) {
    if (!connected || !session.authenticated) return "Нет подключения к панели.";
    if (pendingTaskActions.has(job.id)) return "Предыдущая команда ещё выполняется.";
    if (!canControlTasks() || job.remove_requested === true) return "Дождитесь завершения текущей операции.";
    if (action === "start" && snapshot && !snapshot.authenticated) return "Сначала войдите в аккаунт Mail.ru.";
    if (["start", "return"].includes(action) && job.planned_mode === "regular" && snapshot && snapshot.settings && snapshot.settings.regular_policy !== "upload") {
      return "Для этой задачи требуется квота. Разрешите её использование в настройках.";
    }
    return "";
  }

  function taskPathMatches(job) {
    if (typeof job.cloud_path_matches === "boolean") return job.cloud_path_matches;
    return typeof job.cloud_path === "string" && typeof job.remote_path === "string" && job.cloud_path.normalize("NFC") === job.remote_path.normalize("NFC");
  }

  function taskCloudURL(job) {
    return job.cloud_present !== true || job.cloud_deleted_at ? "" : taskLinkURL(job.cloud_url, true);
  }

  function taskIsCloudStatus(job) {
    return job.status === "uploaded" || job.status === "skipped" && job.skip_reason === "remote_exists";
  }

  function storageDescription(mode) {
    return mode === "unlimited" ? "Хранение: в безлимите." : mode === "regular" ? "Хранение: в квоте." : "Режим хранения не определён.";
  }

  function taskState(job) {
    if (job.remove_requested === true) return ["Удаляется…", "neutral"];
    if (job.paused === true && job.status === "running") return ["Останавливается…", "orange"];
    if (job.paused === true && !["uploaded", "skipped"].includes(job.status)) return jobStates.paused;
    if (job.status === "failed") {
      if (job.cloud_present === true && job.observed_mode === "regular") return ["Ошибка · в квоте", "red"];
      if (job.cloud_deleted_at) return ["Ошибка · в корзине", "red"];
      return jobStates.failed;
    }
    if (job.status === "skipped" && job.skip_reason === "remote_exists") {
      return ["Уже в Облаке", taskPathMatches(job) ? "green" : "yellow"];
    }
    if (job.status === "uploaded") return ["Выполнено", job.observed_mode === "unlimited" ? "green" : job.observed_mode === "regular" ? "yellow" : "neutral"];
    return jobStates[job.status] || ["Неизвестно", "neutral"];
  }

  function taskLinkURL(value, cloud = false) {
    if (typeof value !== "string" || !value) return "";
    try {
      if (cloud) {
        const url = new URL(value);
        return url.protocol === "https:" && url.hostname === "cloud.mail.ru" && !url.username && !url.password && (!url.port || url.port === "443") ? value : "";
      }
      if (!value.startsWith("/") || value.startsWith("//")) return "";
      const url = new URL(value, location.origin);
      return url.origin === location.origin && !url.username && !url.password ? value : "";
    } catch { return ""; }
  }

  function createJobView(job) {
    const row = element("tr", "task-row"); row.dataset.taskId = String(job.id);
    const nameCell = element("td", "task-name-cell");
    const name = element("a", "job-name"), fileIcon = icon("image"), label = element("span");
    name.target = "_blank"; name.rel = "noopener noreferrer"; name.dataset.taskControl = "open";
    name.append(fileIcon, label); nameCell.append(name);
    const progressCell = element("td", "task-progress-cell"), progress = element("div", "task-progress");
    const track = element("div", "progress-track"), fill = element("div", "progress-fill"), percentage = element("span", "task-percent");
    track.setAttribute("role", "progressbar"); track.setAttribute("aria-valuemin", "0"); track.setAttribute("aria-valuemax", "100");
    track.appendChild(fill); progress.append(track, percentage); progressCell.appendChild(progress);
    const size = element("td", "task-size"), statusCell = element("td", "task-status-cell");
    const cloudLink = element("a", "badge neutral task-status-link"); cloudLink.target = "_blank"; cloudLink.rel = "noopener noreferrer"; cloudLink.dataset.taskControl = "status";
    const menu = element("button", "badge neutral task-status-button"); menu.type = "button"; menu.dataset.taskControl = "status";
    const statusLabel = element("span"), statusChevron = icon("chevron", "task-status-chevron"); menu.append(statusLabel, statusChevron);
    menu.setAttribute("aria-haspopup", "menu"); menu.setAttribute("aria-expanded", "false"); menu.setAttribute("aria-controls", "task-action-menu");
    statusCell.append(cloudLink, menu);
    const created = element("td", "task-created task-date"), completed = element("td", "task-completed task-date"), actionsCell = element("td", "task-actions-cell"), actions = element("div", "task-actions");
    const remove = element("button", "icon-button task-remove"); remove.type = "button"; remove.dataset.taskControl = "remove"; remove.appendChild(icon("remove", "small"));
    actions.appendChild(remove); actionsCell.appendChild(actions); row.append(nameCell, progressCell, size, statusCell, created, completed, actionsCell);
    const view = { row, name, fileIcon, label, statusCell, cloudLink, statusLabel, statusChevron, progress, percentage, track, fill, size, created, completed, actions, menu, remove, job, error: "" };
    menu.addEventListener("click", () => actionMenuJobId === view.job.id ? closeTaskMenu(true) : openTaskMenu(view));
    menu.addEventListener("keydown", (event) => { if (["ArrowDown", "ArrowUp"].includes(event.key)) { event.preventDefault(); openTaskMenu(view, event.key === "ArrowUp"); } });
    remove.addEventListener("click", () => { if (remove.getAttribute("aria-disabled") !== "true") openDeletion(view.job); });
    return view;
  }

  function updateJobControls(view) {
    const job = view.job, pending = pendingTaskActions.get(job.id);
    const allowed = canControlTasks() && !pending && job.remove_requested !== true;
    const name = plain(job.relative_path, plain(job.local_path, "Файл"));
    const actions = taskActions(job), cloudURL = taskCloudURL(job);
    const directCloud = taskIsCloudStatus(job) && Boolean(cloudURL);
    view.cloudLink.hidden = !directCloud; view.menu.hidden = directCloud;
    if (cloudURL) view.cloudLink.href = cloudURL;
    else view.cloudLink.removeAttribute("href");
    const actionable = actions.length > 0 || Boolean(cloudURL);
    view.menu.disabled = !actionable;
    view.statusChevron.hidden = !actionable;
    view.menu.setAttribute("aria-label", taskState(job)[0] + ": " + name + (actionable ? ". Действия с задачей." : ""));
    view.menu.classList.toggle("has-error", Boolean(view.error));
    if (view.error) view.menu.title = view.error + "\n" + view.statusCell.title;
    view.remove.setAttribute("aria-label", "Удалить задачу: " + name);
    view.remove.setAttribute("aria-disabled", String(!allowed));
    view.remove.title = "Выбрать, что удалить";
    view.actions.setAttribute("aria-busy", String(Boolean(pending)));
  }

  function updateJobView(view, job) {
    view.job = job;
    const fullName = plain(job.relative_path, plain(job.local_path, "Файл"));
    const name = fullName.split(/[\\/]/).filter(Boolean).pop() || fullName;
    view.label.textContent = name;
    const sourceURL = taskLinkURL(job.preview_url) || taskLinkURL(job.download_url);
    if (sourceURL) { view.name.href = sourceURL; view.name.removeAttribute("aria-disabled"); view.name.removeAttribute("tabindex"); }
    else { view.name.removeAttribute("href"); view.name.setAttribute("aria-disabled", "true"); view.name.tabIndex = -1; }
    view.name.setAttribute("aria-label", "Открыть файл: " + fullName);
    view.fileIcon.firstElementChild.setAttribute("href", "#i-" + mediaIcon(fullName));
    const reason = planReasons[plain(job.planned_reason)] || plain(job.planned_reason);
    const storageHint = job.planned_mode === "regular" ? "Ожидается загрузка в квоту" : job.planned_mode === "unlimited" ? "Кандидат в безлимит" : "";
    view.name.title = [plain(job.local_path, fullName), plain(job.cloud_path, plain(job.remote_path)), storageHint, reason].filter(Boolean).join("\n");
    const state = taskState(job);
    view.cloudLink.textContent = state[0]; view.cloudLink.className = "badge " + state[1] + " task-status-link";
    view.statusLabel.textContent = state[0]; view.menu.className = "badge " + state[1] + " task-status-button";
    let detail = plain(job.last_error) || reason;
    if (job.status === "skipped" && job.skip_reason === "remote_exists") {
      detail = taskPathMatches(job) ? "Файл найден по назначенному пути. Повторная передача не требуется." : "Файл найден в другой папке Облака. Повторная передача не требуется.";
    } else if (job.status === "skipped" && job.skip_reason === "manual_skipped") detail = "Задача пропущена вручную. Её можно вернуть в очередь.";
    if (job.cloud_present === true || taskIsCloudStatus(job)) {
      detail += (detail ? "\n" : "") + storageDescription(job.observed_mode);
      if (job.cloud_path) detail += "\nПуть в Облаке: " + job.cloud_path;
      if (job.status === "skipped" && job.skip_reason === "remote_exists" && !taskPathMatches(job) && job.remote_path) detail += "\nНазначение источника: " + job.remote_path;
    }
    if (job.status === "failed" && job.next_attempt != null) detail += (detail ? " · " : "") + "Повтор: " + dateTime(job.next_attempt);
    view.statusCell.title = detail || state[0]; view.cloudLink.title = detail || state[0]; view.menu.title = detail || state[0];
    view.cloudLink.setAttribute("aria-label", state[0] + ". Показать в Облаке. " + detail);
    view.size.textContent = bytes(job.size);
    view.size.title = count(job.size) + " байт";
    view.created.textContent = dateTime(job.created_at) || "—";
    view.created.title = job.created_at ? "Задача создана: " + dateTime(job.created_at) : "Дата создания неизвестна";
    view.completed.textContent = dateTime(job.completed_at) || "—";
    view.completed.title = job.completed_at ? "Обработка задачи завершена: " + dateTime(job.completed_at) : "Задача ещё не завершена";
    const progress = job.progress && typeof job.progress === "object" ? job.progress : null;
    const completed = job.status === "uploaded" || job.cloud_present === true;
    const total = Math.max(0, finite(progress && progress.total, finite(job.size)));
    const sent = completed ? total : Math.max(0, Math.min(total, finite(progress && progress.sent, finite(job.progress_bytes))));
    const percent = completed ? 100 : Math.max(0, Math.min(100, finite(progress && progress.percent, finite(job.progress_percent, total > 0 ? sent / total * 100 : 0))));
    const paused = job.paused === true || job.status === "paused";
    const stage = completed ? job.skip_reason === "remote_exists" ? "Файл уже в Облаке" : "Передача завершена" : paused ? "На паузе — прогресс сохранён" : progress ? transferStages[plain(progress.stage)] || "Обработка" : job.status === "skipped" ? "Пропущено" : sent > 0 ? "Сохранённый прогресс" : "Передача ещё не началась";
    const progressLabel = job.status === "skipped" && !completed ? "—" : Math.floor(percent) + "%";
    view.percentage.textContent = progressLabel;
    view.track.setAttribute("aria-label", "Передача: " + fullName);
    view.track.setAttribute("aria-valuenow", percent.toFixed(1));
    view.track.setAttribute("aria-valuetext", stage + ": " + progressLabel);
    view.fill.style.width = percent + "%";
    view.progress.classList.toggle("complete", completed);
    view.progress.classList.toggle("paused", paused && !completed);
    view.progress.classList.toggle("inactive", !progress && !completed && sent === 0);
    view.progress.title = stage + " · " + bytes(sent) + " / " + bytes(total);
    updateJobControls(view);
  }

  function restoreTaskFocus(focused, views) {
    if (!focused) return;
    const current = document.activeElement;
    if (current === focused.element && current.isConnected && !current.hidden) return;
    if (current && current !== document.body && current !== focused.element && current.isConnected) return;
    const view = jobViews.get(focused.id) || views[Math.min(focused.index, views.length - 1)];
    const target = view ? focused.control === "remove" ? view.remove : focused.control === "open" ? view.name : view.cloudLink.hidden ? view.menu : view.cloudLink : $("queue-title");
    target.focus({ preventScroll: true });
  }

  function taskMenuItems() { return [...$("task-action-menu").querySelectorAll('[role="menuitem"]')].filter((item) => !item.disabled && !item.hidden); }
  function closeTaskMenu(restoreFocus = false) {
    const view = jobViews.get(actionMenuJobId);
    const hadMenuFocus = $("task-action-menu").contains(document.activeElement);
    if (view) view.menu.setAttribute("aria-expanded", "false");
    actionMenuJobId = null; actionMenuSignature = "";
    setHidden("task-action-menu", true);
    if (restoreFocus && view && view.menu.isConnected) (view.cloudLink.hidden ? view.menu.disabled ? view.name : view.menu : view.cloudLink).focus({ preventScroll: true });
    else if (restoreFocus && hadMenuFocus) $("queue-title").focus({ preventScroll: true });
  }
  function positionTaskMenu() {
    const view = jobViews.get(actionMenuJobId), menu = $("task-action-menu");
    if (!view || menu.hidden) return;
    const anchor = view.menu.getBoundingClientRect(), bounds = menu.getBoundingClientRect();
    const left = Math.max(8, Math.min(innerWidth - bounds.width - 8, anchor.right - bounds.width));
    const top = anchor.bottom + bounds.height + 8 <= innerHeight ? anchor.bottom + 5 : Math.max(8, anchor.top - bounds.height - 5);
    menu.style.left = left + "px"; menu.style.top = top + "px";
  }
  function openTaskMenu(view, last = false) {
    if (view.menu.disabled || view.menu.hidden) return;
    closeTaskMenu(); actionMenuJobId = view.job.id;
    view.menu.setAttribute("aria-expanded", "true"); setHidden("task-action-menu", false);
    updateTaskMenu(); positionTaskMenu();
    const items = taskMenuItems();
    (items.length ? items[last ? items.length - 1 : 0] : $("task-action-menu")).focus();
  }
  function updateTaskMenu() {
    if (actionMenuJobId === null) return;
    const view = jobViews.get(actionMenuJobId);
    if (!view) { closeTaskMenu(true); return; }
    if (view.menu.hidden || view.menu.disabled) { closeTaskMenu(true); return; }
    const job = view.job, actions = taskActions(job), menu = $("task-action-menu");
    const cloudURL = taskCloudURL(job);
    const disabledReasons = actions.map((action) => taskActionDisabledReason(job, action[0]));
    const signature = JSON.stringify([job.id, cloudURL, actions, disabledReasons, view.error, job.last_error]);
    if (signature === actionMenuSignature) return;
    actionMenuSignature = signature;
    const focused = menu.contains(document.activeElement) ? document.activeElement.dataset.menuAction : null;
    menu.replaceChildren();
    const link = (url, symbol, title, key) => {
      if (!url) return;
      const node = element("a", "task-menu-item"); node.href = url; node.target = "_blank"; node.rel = "noopener noreferrer";
      node.setAttribute("role", "menuitem"); node.tabIndex = -1; node.dataset.menuAction = key;
      node.append(icon(symbol, "small"), document.createTextNode(title));
      node.addEventListener("click", () => closeTaskMenu(true)); menu.appendChild(node);
    };
    link(cloudURL, "cloud", "Показать в Облаке", "cloud");
    actions.forEach((action, index) => {
      const button = element("button", "task-menu-item"); button.type = "button";
      button.setAttribute("role", "menuitem"); button.tabIndex = -1; button.dataset.menuAction = action[0]; button.disabled = Boolean(disabledReasons[index]);
      if (disabledReasons[index]) button.title = disabledReasons[index];
      button.append(icon(action[2], "small"), document.createTextNode(action[1]));
      button.addEventListener("click", () => { closeTaskMenu(true); performTask(view, action[0]); }); menu.appendChild(button);
    });
    if (!menu.children.length) menu.appendChild(element("p", "task-menu-note", "Для этой задачи нет доступных действий."));
    const disabledNote = disabledReasons.find(Boolean);
    if (disabledNote) menu.appendChild(element("p", "task-menu-note", disabledNote));
    const error = view.error || plain(job.last_error);
    if (error) menu.appendChild(element("p", "task-menu-note error", error));
    if (focused) {
      const item = [...menu.querySelectorAll("[data-menu-action]")].find((node) => node.dataset.menuAction === focused && !node.disabled);
      (item || taskMenuItems()[0] || view.menu).focus({ preventScroll: true });
    }
    positionTaskMenu();
  }

  function renderJobs(jobs) {
    const body = $("jobs-body");
    const focusedElement = document.activeElement;
    const focusedRow = focusedElement && focusedElement.closest ? focusedElement.closest("#jobs-body tr") : null;
    const focused = focusedRow ? { element: focusedElement, id: Number(focusedRow.dataset.taskId), index: [...body.children].indexOf(focusedRow), control: focusedElement.dataset.taskControl } : null;
    const present = new Set(), views = [];
    jobs.forEach((job) => {
      if (!Number.isSafeInteger(job.id) || job.id <= 0 || present.has(job.id)) return;
      present.add(job.id);
      const view = jobViews.get(job.id) || createJobView(job);
      jobViews.set(job.id, view); updateJobView(view, job); views.push(view);
    });
    for (const [id, view] of jobViews) {
      if (!present.has(id)) { view.row.remove(); jobViews.delete(id); }
    }
    views.forEach((view, index) => {
      if (body.children[index] !== view.row) body.insertBefore(view.row, body.children[index] || null);
    });
    setHidden("jobs-empty", jobs.length > 0);
    setText("jobs-empty-title", jobsFilter === "all" ? "В очереди пока нет задач" : "Таких задач сейчас нет");
    setText("jobs-empty-description", jobsFilter === "all" ? "Добавьте исходную папку и запустите сканирование." : "Выберите другой фильтр, чтобы посмотреть остальные задачи.");
    setText("jobs-range", jobsTotal === 0 ? "0 задач" : "Задачи " + count(jobsOffset + 1) + "–" + count(jobsOffset + jobs.length) + " из " + count(jobsTotal));
    $("jobs-prev").disabled = jobsOffset === 0;
    $("jobs-next").disabled = jobsOffset + jobs.length >= jobsTotal;
    setHidden("task-controls-note", snapshot && snapshot.task_controls_available === true || !connected || jobsTotal === 0);
    setText("jobs-summary-count", taskCount(jobsTotal));
    setText("jobs-summary-bytes", jobsTotalBytes === null ? "Объём неизвестен" : bytes(jobsTotalBytes));
    restoreTaskFocus(focused, views);
    updateTaskMenu();
  }

  async function performTask(view, action) {
    const job = view.job;
    if (!canControlTasks() || pendingTaskActions.has(job.id) || job.remove_requested === true) return;
    if (!taskActions(job).some((item) => item[0] === action) || taskActionDisabledReason(job, action)) return;
    if (action === "pause" && job.paused === true) return;
    pendingTaskActions.set(job.id, action); view.error = ""; updateControls();
    let accepted = false;
    try {
      await api("/api/tasks/" + action, { method: "POST", body: { id: job.id } });
      accepted = true;
      await refreshState();
      if (activePage === "queue") await loadJobs(false);
      const messages = { pause: "Останавливаем выбранную задачу.", start: "Выбранная задача запущена.", skip: "Задача пропущена. Её можно вернуть в очередь.", return: "Задача возвращена в очередь." };
      toast(messages[action]);
    } catch (error) {
      view.error = accepted ? "Команда принята, но состояние не удалось обновить. " + error.message : error.message;
      toast(view.error, true);
    } finally {
      pendingTaskActions.delete(job.id); updateControls(); schedulePoll(true);
    }
  }

  function openQueueExport() {
    if (!canExportQueue()) return;
    const filters = ["all", "uploaded_unlimited", "uploaded_regular", "failed", "skipped"];
    $("export-filter").value = filters.includes(jobsFilter) ? jobsFilter : "all";
    setText("export-sort-description", "Порядок строк: «" + jobsSortFields[jobsSortBy] + "», " + (jobsSort === "asc" ? "по возрастанию." : "по убыванию."));
    setError("export-error"); showDialog("export-dialog");
  }

  function downloadQueueExport(event) {
    event.preventDefault(); setError("export-error");
    if (!canExportQueue()) { setError("export-error", "Дождитесь завершения сканирования и проверьте подключение к панели."); return; }
    const fields = [...document.querySelectorAll('[name="export-field"]:checked')].map((input) => input.value).filter((value) => ["path", "name", "size", "status"].includes(value));
    if (fields.length === 0) {
      setError("export-error", "Выберите хотя бы один столбец.");
      document.querySelector('[name="export-field"]').focus(); return;
    }
    const params = new URLSearchParams({ status: $("export-filter").value, fields: fields.join(","),
      format: $("export-format").value, delimiter: $("export-delimiter").value,
      header: $("export-header").checked ? "1" : "0", sort_by: jobsSortBy, sort: jobsSort });
    // Let the browser stream a large export to disk instead of materializing
    // hundreds of thousands of rows in this tab's JavaScript memory.
    const download = document.createElement("a");
    download.href = "/api/jobs/export?" + params.toString();
    download.download = "mail-cloud-tasks." + ($("export-format").value === "txt" ? "txt" : "csv");
    download.hidden = true; document.body.appendChild(download); download.click(); download.remove();
    $("export-dialog").close(); toast("Загрузка списка начата.");
  }

  function deleteRequestBody() {
    if (!deleteContext) return null;
    const body = { delete_cloud: $("delete-cloud").checked, delete_local: $("delete-local").checked };
    if (deleteContext.id !== null) body.id = deleteContext.id;
    else {
      body.scope = $("queue-clear-scope").value;
      if (body.scope === "filter") body.status = deleteContext.filter;
    }
    return body;
  }

  function updateDeletionControls() {
    const submitting = pendingActions.has("delete"), available = canControlTasks() || canClearQueue();
    const physical = !isBusy() && !anyAction() && connected;
    const localSupported = Boolean(snapshot && snapshot.local_delete_available === true);
    $("queue-clear-options").disabled = submitting || !available;
    $("delete-cloud").disabled = submitting || !physical || !snapshot || !snapshot.authenticated;
    $("delete-local").disabled = submitting || !physical || !localSupported;
    $("delete-local-confirm").disabled = submitting;
    setHidden("queue-clear-busy", !isBusy());
    setHidden("delete-local-unavailable", localSupported);
    setHidden("delete-local-confirm-wrap", !$("delete-local").checked);
    const physicalSelected = $("delete-cloud").checked || $("delete-local").checked;
    const validPreview = Boolean(deletePreview && typeof deletePreview.token === "string" && deletePreview.token && finite(deletePreview.task_count) > 0);
    const localAllowed = !$("delete-local").checked || Boolean(deletePreview && deletePreview.local_delete_allowed === true && $("delete-local-confirm").value === "УДАЛИТЬ С ДИСКА");
    $("queue-clear-submit").disabled = submitting || !available || deletePreviewLoading || !validPreview || !localAllowed || physicalSelected && !physical;
    setText("queue-clear-submit", submitting ? "Удаляем…" : "Удалить");
    $("queue-clear-dialog").querySelectorAll("[data-close-dialog]").forEach((button) => { button.disabled = submitting; });
  }

  function openDeletion(job = null) {
    if (job ? !canControlTasks() : !canClearQueue()) return;
    closeTaskMenu();
    deleteContext = { id: job ? job.id : null, filter: jobsFilter };
    deletePreview = null; deletePreviewRequest += 1;
    $("queue-clear-form").reset();
    setHidden("queue-clear-options", Boolean(job));
    setText("queue-clear-title", job ? "Удалить задачу?" : "Удалить задачи?");
    setText("queue-clear-description", job ? plain(job.relative_path, plain(job.local_path, "Выбранная задача")) : "Выберите задачи и укажите, что удалить вместе с ними. Исходные папки останутся подключёнными.");
    const tab = [...document.querySelectorAll(".filter-tab")].find((node) => node.dataset.filter === jobsFilter);
    $("queue-clear-scope").querySelector('option[value="filter"]').textContent = "Текущий фильтр: " + (tab ? tab.textContent : "Все");
    setError("queue-clear-error");
    showDialog("queue-clear-dialog");
    scheduleDeletePreview(0);
  }

  function scheduleDeletePreview(delay = 120) {
    clearTimeout(deletePreviewTimer); deletePreviewRequest += 1; deletePreview = null;
    deletePreviewLoading = true;
    setError("queue-clear-error");
    setText("delete-preview-summary", "Подсчитываем выбранные задачи…");
    setText("delete-preview-files", "");
    $("delete-preview-warnings").replaceChildren(); setHidden("delete-preview-warnings", true);
    updateDeletionControls();
    if (!deleteContext || !$("queue-clear-dialog").open) return;
    deletePreviewTimer = setTimeout(refreshDeletePreview, delay);
  }

  async function refreshDeletePreview() {
    const body = deleteRequestBody();
    if (!body || !$("queue-clear-dialog").open) return;
    const request = ++deletePreviewRequest;
    deletePreviewLoading = true; updateDeletionControls();
    try {
      const result = await api("/api/tasks/delete/preview", { method: "POST", body });
      if (request !== deletePreviewRequest || !deleteContext || !$("queue-clear-dialog").open) return;
      deletePreview = result;
      setText("delete-preview-summary", taskCount(result.task_count) + " · " + bytes(result.total_bytes));
      const detail = [];
      if (body.delete_cloud) detail.push("В корзину Облака: " + fileCount(result.cloud_count));
      if (body.delete_local) detail.push("Навсегда с диска: " + fileCount(result.local_count));
      if (!body.delete_cloud && !body.delete_local) detail.push("Файлы на диске и в Облаке сохранятся.");
      setText("delete-preview-files", detail.join(". "));
      const warnings = Array.isArray(result.warnings) ? result.warnings.filter((value) => typeof value === "string") : [];
      if (body.delete_local && result.local_delete_allowed !== true && !warnings.length) warnings.push("Удаление с диска недоступно. Проверьте права и разрешение записи для исходной папки.");
      const list = $("delete-preview-warnings"); list.replaceChildren(...warnings.map((message) => element("li", "", message)));
      list.hidden = warnings.length === 0;
    } catch (error) {
      if (request !== deletePreviewRequest) return;
      deletePreview = null; setText("delete-preview-summary", "Не удалось подготовить удаление.");
      setError("queue-clear-error", error.message);
    } finally {
      if (request === deletePreviewRequest) { deletePreviewLoading = false; updateDeletionControls(); }
    }
  }

  async function submitDeletion(event) {
    event.preventDefault(); updateDeletionControls();
    if ($("queue-clear-submit").disabled || !deletePreview || !deleteContext) return;
    if (Number.isFinite(deletePreview.expires_at) && deletePreview.expires_at <= Date.now() / 1000) {
      scheduleDeletePreview(0); toast("Список обновляется. Проверьте итог перед удалением."); return;
    }
    const body = { token: deletePreview.token };
    if ($("delete-local").checked) body.local_confirmation = $("delete-local-confirm").value;
    pendingActions.add("delete"); updateControls(); setError("queue-clear-error");
    try {
      await api("/api/tasks/delete", { method: "POST", body });
      $("queue-clear-dialog").close(); jobsOffset = 0;
      await refreshState();
      if (activePage === "queue") await loadJobs(false);
      toast("Удаление выбранных задач запущено. Результат появится в журнале.");
      $("queue-title").focus({ preventScroll: true });
    } catch (error) {
      setError("queue-clear-error", error.message);
      deletePreview = null;
      if ($("queue-clear-dialog").open) await refreshDeletePreview();
    } finally {
      pendingActions.delete("delete"); updateControls(); schedulePoll(true);
    }
  }

  async function perform(key, path, body = {}, options = {}) {
    if (pendingActions.has(key)) return false;
    pendingActions.add(key); updateControls();
    if (options.errorId) setError(options.errorId);
    try {
      await api(path, { method: "POST", body });
      if (options.resetJobs) jobsOffset = 0;
      if (options.close) $(options.close).close();
      await refreshState();
      if (activePage === "queue") await loadJobs(false);
      if (options.message) toast(options.message);
      return true;
    } catch (error) {
      if (options.errorId) setError(options.errorId, error.message);
      else toast(error.message, true);
      return false;
    } finally { pendingActions.delete(key); updateControls(); schedulePoll(true); }
  }

  function openMail() {
    closeAccountMenu();
    if (!snapshot || isBusy() || anyAction() || !connected) return;
    $("mail-email").value = plain(snapshot.account);
    $("mail-password").value = "";
    setError("mail-error"); showDialog("mail-dialog");
  }
  function sourceById(id) {
    return snapshot && Array.isArray(snapshot.sources) ? snapshot.sources.find((source) => source.id === id) : null;
  }
  function openSource(id = null) {
    if (!snapshot || isBusy() || anyAction() || !connected) return;
    const source = id === null ? null : sourceById(id);
    if (id !== null && !source) { toast("Источник уже убран. Список папок обновлён."); return; }
    editingSourceId = source ? source.id : null;
    $("source-form").reset();
    $("source-path").value = source ? plain(source.path) : "";
    $("source-path").readOnly = Boolean(source);
    $("source-alias").value = source ? plain(source.alias) : "";
    $("source-remote-prefix").value = source ? plain(source.remote_prefix) : "";
    setText("source-dialog-label", source ? "НАСТРОЙКИ ИСТОЧНИКА" : "НОВЫЙ ИСТОЧНИК");
    setText("source-dialog-title", source ? "Изменить источник" : "Добавить папку с фото и видео");
    setText("source-description", source ? "Измените название источника или назначение загрузки в Облаке." : "Файлы читаются прямо из этой папки. Все вложенные каталоги будут найдены автоматически.");
    setText("source-submit", source ? "Сохранить изменения" : "Добавить источник");
    setHidden("browse-folders", Boolean(source));
    setHidden("source-path-hint", !source);
    setHidden("source-update-note", !source);
    setError("source-error"); updateSourcePreview(); updateControls(); showDialog("source-dialog");
    if (source) $("source-alias").focus();
  }
  function updateSourcePreview() {
    const remote = $("source-remote-prefix").value.trim();
    setText("source-remote-hint", remote.startsWith("/") ? "Например, файл 2024/IMG.jpg будет загружен в " + remote.replace(/\/+$/, "") + "/2024/IMG.jpg. Название источника к пути не добавляется." : "Полный путь от корня Облака, начиная с /. Например: /Фото/Семья. Название источника к пути не добавляется.");
  }
  function openSourceRemoval(id) {
    if (!snapshot || isBusy() || anyAction() || !connected) return;
    const source = sourceById(id);
    if (!source) { toast("Источник уже убран. Список папок обновлён."); return; }
    removingSourceId = source.id;
    setText("source-remove-name", plain(source.alias, "Папка"));
    setText("source-remove-path", "На сервере: " + plain(source.path));
    setText("source-remove-remote", "В Облаке: " + plain(source.remote_prefix));
    setError("source-remove-error"); updateControls(); showDialog("source-remove-dialog");
  }
  async function openFolderPicker() {
    if (editingSourceId !== null || isBusy() || anyAction() || !connected) return;
    setError("folder-error"); showDialog("folder-dialog");
    await browseFolder(null);
  }
  async function browseFolder(path) {
    const request = ++folderRequest;
    setError("folder-error");
    $("folder-select").disabled = true; $("folder-up").disabled = true;
    $("folder-list").replaceChildren(element("div", "folder-empty", "Читаем список папок…"));
    try {
      const result = await api("/api/folders" + (path ? "?path=" + encodeURIComponent(path) : ""));
      if (request !== folderRequest) return;
      folderState = { path: typeof result.path === "string" ? result.path : null, parent: typeof result.parent === "string" ? result.parent : null };
      setText("folder-path", folderState.path || "Доступные папки сервера");
      $("folder-up").disabled = !folderState.parent;
      $("folder-select").disabled = !folderState.path;
      const roots = Array.isArray(result.roots) ? result.roots : [];
      const directories = Array.isArray(result.directories) ? result.directories : [];
      const entries = folderState.path ? directories : roots.length ? roots : directories;
      const container = $("folder-list"); container.replaceChildren();
      entries.forEach((entry) => {
        if (typeof entry.path !== "string") return;
        const row = element("button", "folder-row"); row.type = "button";
        row.append(icon("folder"), element("span", "", plain(entry.name, entry.path)), icon("chevron"));
        row.addEventListener("click", () => browseFolder(entry.path));
        container.appendChild(row);
      });
      if (!container.childElementCount) container.appendChild(element("div", "folder-empty", folderState.path ? "Вложенных папок нет. Можно выбрать текущую папку — файлы будут найдены при сканировании." : "Нет доступных папок. Подключите каталог с фото и видео к серверу и разрешите к нему доступ в настройках запуска."));
      setText("folder-note", result.truncated ? "Показана часть каталогов. Нужный полный путь также можно ввести в поле источника." : "Показаны только папки. Сами фото и видео здесь не перечисляются.");
    } catch (error) {
      if (request !== folderRequest) return;
      folderState = { path: null, parent: null };
      setText("folder-path", "Не удалось открыть папку");
      $("folder-list").replaceChildren(element("div", "folder-empty", "Выберите «Доступные диски» или попробуйте ещё раз."));
      setError("folder-error", error.message);
    }
  }

  document.querySelectorAll("[data-close-dialog]").forEach((button) => button.addEventListener("click", () => button.closest("dialog").close()));
  document.querySelectorAll("dialog").forEach((dialog) => dialog.addEventListener("close", () => setTimeout(maybeShowScanWarning, 0)));
  $("queue-clear-dialog").addEventListener("cancel", (event) => { if (pendingActions.has("delete")) event.preventDefault(); });
  $("task-action-menu").addEventListener("keydown", (event) => {
    if (event.key === "Escape") { event.preventDefault(); closeTaskMenu(true); }
    else if (event.key === "Tab") closeTaskMenu(true);
    else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      const items = taskMenuItems(); if (!items.length) return;
      const current = items.indexOf(document.activeElement);
      const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 : (current + (event.key === "ArrowUp" ? -1 : 1) + items.length) % items.length;
      items[next].focus();
    }
  });
  document.addEventListener("pointerdown", (event) => {
    const view = jobViews.get(actionMenuJobId);
    if (view && !$("task-action-menu").contains(event.target) && !view.menu.contains(event.target)) closeTaskMenu();
  });
  document.addEventListener("focusin", (event) => {
    const view = jobViews.get(actionMenuJobId);
    if (view && !$("task-action-menu").contains(event.target) && event.target !== view.menu) closeTaskMenu();
  });
  window.addEventListener("resize", () => closeTaskMenu());
  document.querySelector(".table-scroll").addEventListener("scroll", () => closeTaskMenu());
  $("mail-dialog").addEventListener("close", () => { $("mail-password").value = ""; });
  $("panel-password-dialog").addEventListener("close", clearPanelPasswords);
  window.addEventListener("hashchange", () => navigate(location.hash.slice(1)));
  document.addEventListener("visibilitychange", () => {
    clearTimeout(pollTimer);
    if (!document.hidden && session.authenticated) { poll(); maybeShowScanWarning(); }
  });

  $("access-form").addEventListener("submit", async (event) => {
    event.preventDefault(); if (accessBusy) return;
    setError("access-error");
    if (session.setup_required && $("access-password").value !== $("access-confirm").value) { setError("access-error", "Пароли не совпадают."); return; }
    if (session.setup_required && $("access-password").value.length < 12) { setError("access-error", "Для панели нужен пароль длиной не менее 12 символов."); return; }
    accessBusy = true; $("access-submit").disabled = true;
    try {
      const request = api(session.setup_required ? "/api/session/setup" : "/api/session/login", { method: "POST", body: { password: $("access-password").value } });
      $("access-password").value = ""; $("access-confirm").value = "";
      const result = await request;
      if (result && typeof result.csrf === "string") session.csrf = result.csrf;
      if (await refreshSession()) { await refreshState(); navigate(location.hash.slice(1)); poll(); }
    } catch (error) { setError("access-error", error.message); }
    finally { accessBusy = false; $("access-submit").disabled = false; }
  });

  async function logoutPanel() {
    if (pendingActions.has("password")) return;
    try {
      await api("/api/session/logout", { method: "POST" });
      session.authenticated = false; snapshot = null;
      await refreshSession(); setError("access-error");
    } catch (error) { toast(error.message, true); }
  }
  function accountMenuItems() { return [...$("account-menu").querySelectorAll('[role="menuitem"]')].filter((item) => !item.disabled && !item.hidden); }
  function closeAccountMenu(restoreFocus = false) {
    const wasOpen = !$("account-menu").hidden;
    setHidden("account-menu", true);
    $("account-button").setAttribute("aria-expanded", "false");
    if (wasOpen && restoreFocus) $("account-button").focus();
  }
  function openAccountMenu(last = false) {
    if (!snapshot) return;
    updateControls();
    if ($("account-button").disabled) return;
    setHidden("account-menu", false);
    $("account-button").setAttribute("aria-expanded", "true");
    const items = accountMenuItems();
    if (items.length) items[last ? items.length - 1 : 0].focus();
  }
  async function logoutMail() {
    closeAccountMenu(true);
    mailLogoutRequested = true;
    if (!await perform("logout", "/api/logout")) mailLogoutRequested = false;
  }
  $("account-button").addEventListener("click", () => {
    if (!$("account-menu").hidden) closeAccountMenu(true);
    else openAccountMenu();
  });
  $("account-button").addEventListener("keydown", (event) => {
    if (["ArrowDown", "ArrowUp"].includes(event.key) && snapshot) {
      event.preventDefault(); openAccountMenu(event.key === "ArrowUp");
    }
  });
  $("account-menu").addEventListener("keydown", (event) => {
    if (event.key === "Escape") { event.preventDefault(); closeAccountMenu(true); }
    else if (event.key === "Tab") closeAccountMenu(true);
    else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      const items = accountMenuItems();
      if (!items.length) return;
      const current = items.indexOf(document.activeElement);
      const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 :
        (current + (event.key === "ArrowUp" ? -1 : 1) + items.length) % items.length;
      items[next].focus();
    }
  });
  document.addEventListener("pointerdown", (event) => {
    if (!$("account-menu-wrap").contains(event.target)) closeAccountMenu();
  });
  $("account-menu-wrap").addEventListener("focusout", (event) => {
    if (!$("account-menu-wrap").contains(event.relatedTarget)) closeAccountMenu();
  });
  $("account-menu-login").addEventListener("click", openMail);
  $("account-menu-logout").addEventListener("click", logoutMail);
  ["logout-panel", "logout-panel-settings"].forEach((id) => $(id).addEventListener("click", logoutPanel));
  $("refresh-connection").addEventListener("click", () => { clearTimeout(pollTimer); poll(); });
  ["start-upload", "queue-start-upload"].forEach((id) => $(id).addEventListener("click", () => perform("start", "/api/start")));
  ["pause-upload", "queue-pause-upload"].forEach((id) => $(id).addEventListener("click", () => perform("pause", "/api/pause", {}, { message: "Останавливаем текущую работу и сохраняем прогресс…" })));
  ["scan-folders", "queue-scan-folders"].forEach((id) => $(id).addEventListener("click", () => perform("scan", "/api/scan")));
  $("view-errors").addEventListener("click", () => { setFilter("failed"); location.hash = "queue"; if (activePage === "queue") loadJobs(); });
  $("view-existing").addEventListener("click", () => { setFilter("existing"); location.hash = "queue"; if (activePage === "queue") loadJobs(); });
  ["onboarding-login"].forEach((id) => $(id).addEventListener("click", openMail));
  ["add-source", "empty-add-source", "onboarding-source"].forEach((id) => $(id).addEventListener("click", () => openSource()));
  $("mail-form").addEventListener("submit", async (event) => {
    event.preventDefault(); if (pendingActions.has("login")) return;
    $("mail-submit").disabled = true;
    try {
      const request = perform("login", "/api/login", { email: $("mail-email").value.trim(), password: $("mail-password").value }, { errorId: "mail-error", close: "mail-dialog", message: "Проверяем вход в Облако…" });
      $("mail-password").value = "";
      await request;
    } finally { $("mail-submit").disabled = false; }
  });
  $("source-remote-prefix").addEventListener("input", updateSourcePreview);
  $("browse-folders").addEventListener("click", openFolderPicker);
  $("folder-roots").addEventListener("click", () => browseFolder(null));
  $("folder-up").addEventListener("click", () => { if (folderState.parent) browseFolder(folderState.parent); });
  $("folder-select").addEventListener("click", () => {
    if (!folderState.path) return;
    $("source-path").value = folderState.path;
    if (!$("source-alias").value.trim()) $("source-alias").value = folderState.path.split(/[\\/]/).filter(Boolean).pop() || "Photos";
    updateSourcePreview(); $("folder-dialog").close(); $("source-alias").focus();
  });
  $("source-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (isBusy() || anyAction() || !connected) return;
    const alias = $("source-alias").value.trim(), remote_prefix = $("source-remote-prefix").value.trim();
    if (!alias) { setError("source-error", "Укажите название источника."); $("source-alias").focus(); return; }
    if (!remote_prefix.startsWith("/")) { setError("source-error", "Укажите полный путь в Облаке, начиная с /. Например: /Фото/Семья."); $("source-remote-prefix").focus(); return; }
    const editing = editingSourceId !== null;
    const body = editing ? { id: editingSourceId, remote_prefix, alias } : { path: $("source-path").value.trim(), alias, remote_prefix };
    if (await perform("source", editing ? "/api/sources/update" : "/api/sources/add", body, { errorId: "source-error", close: "source-dialog", message: editing ? "Настройки источника сохранены. Уже загруженные файлы остаются на прежних местах." : "Папка добавлена. Нажмите «Начать загрузку» или запустите сканирование." })) {
      $("add-source").focus();
    }
  });
  $("source-remove-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (removingSourceId === null || isBusy() || anyAction() || !connected) return;
    if (await perform("source-remove", "/api/sources/remove", { id: removingSourceId }, { errorId: "source-remove-error", close: "source-remove-dialog", message: "Источник и его задачи убраны. Файлы на сервере и в Облаке сохранены." })) {
      removingSourceId = null;
      $("add-source").focus();
    }
  });

  function setFilter(filter) {
    closeTaskMenu();
    jobsFilter = ["all", "pending", "running", "paused", "uploaded_unlimited", "uploaded_regular", "existing", "failed", "skipped"].includes(filter) ? filter : "all";
    jobsOffset = 0;
    document.querySelectorAll(".filter-tab").forEach((button) => { const active = button.dataset.filter === jobsFilter; button.classList.toggle("active", active); button.setAttribute("aria-pressed", String(active)); });
  }
  document.querySelectorAll(".filter-tab").forEach((button) => button.addEventListener("click", () => { setFilter(button.dataset.filter); loadJobs(); }));
  document.querySelectorAll("[data-sort-by]").forEach((button) => button.addEventListener("click", () => {
    const field = button.dataset.sortBy;
    if (!Object.hasOwn(jobsSortFields, field)) return;
    closeTaskMenu(); jobsSort = jobsSortBy === field && jobsSort === "asc" ? "desc" : "asc"; jobsSortBy = field;
    jobsOffset = 0; saveJobsPreferences(); renderJobsSort(); loadJobs();
  }));
  $("jobs-limit").addEventListener("change", () => {
    const value = Number($("jobs-limit").value);
    closeTaskMenu(); jobsLimit = jobsPageSizes.includes(value) ? value : 50; jobsOffset = 0;
    saveJobsPreferences(); renderJobsSort(); loadJobs();
  });
  $("jobs-prev").addEventListener("click", () => { jobsOffset = Math.max(0, jobsOffset - jobsLimit); loadJobs(); });
  $("jobs-next").addEventListener("click", () => { if (jobsOffset + jobsLimit < jobsTotal) { jobsOffset += jobsLimit; loadJobs(); } });
  $("retry-errors").addEventListener("click", () => { $("retry-permanent").checked = false; setError("retry-error"); showDialog("retry-dialog"); });
  $("retry-form").addEventListener("submit", async (event) => {
    event.preventDefault(); $("retry-submit").disabled = true;
    try { await perform("retry", "/api/retry", { include_permanent: $("retry-permanent").checked }, { errorId: "retry-error", close: "retry-dialog", message: "Подходящие задачи возвращены в очередь." }); }
    finally { $("retry-submit").disabled = false; }
  });
  $("clear-queue").addEventListener("click", () => openDeletion());
  $("export-queue").addEventListener("click", openQueueExport);
  $("export-form").addEventListener("submit", downloadQueueExport);
  ["queue-clear-scope", "delete-cloud", "delete-local"].forEach((id) => $(id).addEventListener("change", () => {
    $("delete-local-confirm").value = "";
    scheduleDeletePreview();
  }));
  $("delete-local-confirm").addEventListener("input", updateDeletionControls);
  $("queue-clear-dialog").addEventListener("close", () => {
    deleteContext = null; deletePreview = null; deletePreviewRequest += 1; deletePreviewLoading = false;
    clearTimeout(deletePreviewTimer); $("delete-local-confirm").value = "";
  });
  $("queue-clear-form").addEventListener("submit", submitDeletion);
  $("scan-warning-settings").addEventListener("click", () => { $("scan-warning-dialog").close(); location.hash = "settings"; });
  ["add-schedule", "empty-add-schedule"].forEach((id) => $(id).addEventListener("click", () => openSchedule()));
  ["schedule-weekly", "schedule-monthly"].forEach((id) => $(id).addEventListener("change", updateScheduleMode));
  $("schedule-select-all").addEventListener("click", () => document.querySelectorAll('[name="schedule-source"]').forEach((input) => { input.checked = true; }));
  $("schedule-select-none").addEventListener("click", () => document.querySelectorAll('[name="schedule-source"]').forEach((input) => { input.checked = false; }));
  $("schedule-form").addEventListener("submit", saveSchedule);
  $("schedule-remove-form").addEventListener("submit", async (event) => {
    event.preventDefault(); if (removingScheduleId === null) return;
    await mutateSchedule("/api/schedules/remove", { id: removingScheduleId }, { errorId: "schedule-remove-error", close: "schedule-remove-dialog", message: "Расписание удалено." });
  });
  ["schedule-dialog", "schedule-remove-dialog"].forEach((id) => $(id).addEventListener("cancel", (event) => { if (pendingActions.has("schedule")) event.preventDefault(); }));
  $("quota-refresh").addEventListener("click", () => {
    if (!snapshot || snapshot.quota_refresh_available !== true || anyAction() || !connected) return;
    quotaAutoRequested = true;
    perform("quota", "/api/quota/refresh", {}, { message: "Обновление данных о месте запрошено." });
  });

  function clearPanelPasswords() {
    ["panel-password-current", "panel-password-new", "panel-password-confirm"].forEach((id) => { $(id).value = ""; });
  }

  $("change-panel-password").addEventListener("click", () => {
    if (anyAction() || !connected || !session.authenticated) return;
    clearPanelPasswords(); setError("panel-password-error"); showDialog("panel-password-dialog");
  });
  $("panel-password-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (anyAction() || !connected || !session.authenticated) return;
    setError("panel-password-error");
    const currentPassword = $("panel-password-current").value, newPassword = $("panel-password-new").value;
    if (!currentPassword) { setError("panel-password-error", "Введите текущий пароль панели."); $("panel-password-current").focus(); return; }
    if (Array.from(newPassword).length < 12 || Array.from(newPassword).length > 1024) { setError("panel-password-error", "Новый пароль должен содержать от 12 до 1024 символов."); $("panel-password-new").focus(); return; }
    if (newPassword !== $("panel-password-confirm").value) { setError("panel-password-error", "Новые пароли не совпадают."); $("panel-password-confirm").focus(); return; }
    pendingActions.add("password"); sessionGeneration += 1; clearTimeout(pollTimer); updateControls();
    try {
      const result = await api("/api/session/password", { method: "POST", body: { current_password: currentPassword, new_password: newPassword } });
      if (result.authenticated === true && typeof result.csrf === "string" && result.csrf) {
        session = { authenticated: true, setup_required: false, csrf: result.csrf };
      } else if (!await refreshSession()) {
        return;
      }
      $("panel-password-dialog").close();
      toast("Пароль панели изменён. В этом браузере вход сохранён.");
    } catch (error) {
      if ($("panel-password-dialog").open) setError("panel-password-error", error.message);
      else toast(error.message, true);
    } finally {
      clearPanelPasswords(); pendingActions.delete("password"); updateControls();
      clearTimeout(pollTimer); poll();
    }
  });

  ["workers", "auto-resume", "remote-reconcile", "regular-policy", "unexpected-unlimited-action", "timezone"].forEach((id) => $(id).addEventListener("input", () => { settingsDirty = true; }));
  $("settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const workers = Number($("workers").value);
    if (!Number.isInteger(workers) || workers < 1 || workers > 16) { setError("settings-error", "Укажите целое число потоков от 1 до 16."); return; }
    const success = await perform("settings", "/api/settings", { workers, auto_resume: $("auto-resume").checked, regular_policy: $("regular-policy").value, unexpected_unlimited_action: $("unexpected-unlimited-action").value, remote_reconcile: $("remote-reconcile").checked, timezone: $("timezone").value.trim() }, { errorId: "settings-error", message: "Настройки сохранены." });
    if (success) { settingsDirty = false; if (snapshot) renderSettings(snapshot); }
  });

  renderJobsSort();
  navigate(location.hash.slice(1));
  (async () => {
    try {
      if (await refreshSession()) { await refreshState(); navigate(location.hash.slice(1)); poll(); }
    } catch (error) {
      if (session.authenticated) { setHidden("loading-screen", true); setHidden("app", false); setConnectivity(false, error.message); poll(); }
      else { showAccess(false); setError("access-error", error.message + " Обновите страницу после запуска сервера."); }
    }
  })();
})();
