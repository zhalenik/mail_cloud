"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const numberFormat = new Intl.NumberFormat("ru-RU");
  const pageNames = { overview: "Обзор", queue: "Очередь задач", sources: "Исходные папки", settings: "Настройки" };
  const busyPhases = new Set(["login", "logout", "scanning", "checking", "probing", "uploading", "pausing", "reconciling"]);
  const phaseInfo = {
    idle: ["Готов к работе", "Ожидание", "neutral", "cloud"],
    login: ["Подключаем аккаунт", "Подключение", "blue", "lock"],
    logout: ["Выходим из Mail.ru", "Выход", "orange", "logout"],
    scanning: ["Ищем фото и видео", "Сканирование", "blue", "scan"],
    checking: ["Проверяем доступ к Облаку", "Проверка", "blue", "lock"],
    probing: ["Проверяем безлимит", "Проверка", "blue", "lock"],
    reconciling: ["Проверяем файлы в Облаке", "Проверка", "blue", "scan"],
    uploading: ["Загружаем файлы", "Загрузка", "blue", "cloud"],
    pausing: ["Сохраняем прогресс", "Останавливаем", "orange", "pause"],
    paused: ["Загрузка приостановлена", "На паузе", "orange", "pause"],
    completed: ["Обработка завершена", "Завершено", "green", "check"],
    error: ["Нужно ваше внимание", "Приостановлено", "red", "alert"]
  };
  const jobStates = {
    pending: ["Ожидает", "neutral"], running: ["В работе", "blue"], uploaded: ["Выполнена", "green"],
    failed: ["Ошибка", "red"], skipped: ["Пропущена", "orange"], paused: ["На паузе", "orange"]
  };
  const transferStages = { hash: "Проверка файла", hashing: "Проверка файла", upload: "Передача", uploading: "Передача", blob_upload: "Передача", stat: "Проверка результата", verify: "Проверка результата", verifying: "Проверка результата", commit: "Подтверждение", add: "Подтверждение", unlimited_add: "Подтверждение", preparing: "Подготовка" };
  let session = { authenticated: false, setup_required: false, csrf: "" };
  let snapshot = null;
  let activePage = "overview";
  let pollTimer = null;
  let pollInFlight = false;
  let toastTimer = null;
  let jobsFilter = "all";
  let jobsOffset = 0;
  let jobsTotal = 0;
  let jobsRequest = 0;
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
  const jobsLimit = 50;

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
    return new Date(value * 1000).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
  }
  function plain(value, fallback = "") { return typeof value === "string" ? value : fallback; }
  function mediaIcon(path) { return /\.(mp4|3gp|3g2|3gpp|3gpp2|avi|vob|mov|mkv|m2t|m2ts|ts|mts|f4v|flv|wmv|m4v|mpg|ogv|webm|hevc)$/i.test(path) ? "video" : "image"; }
  function setText(id, value) { $(id).textContent = String(value); }
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
    const label = element("span", "storage-mode", "Обычное хранение");
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
  }

  async function api(path, options = {}) {
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
        if (response.status === 401 && !path.startsWith("/api/session")) {
          session.authenticated = false;
          showAccess(false);
          setError("access-error", "Сессия панели завершилась. Войдите снова.");
        }
        const message = typeof value.error === "string" ? value.error : plain(value.error && value.error.message);
        const error = new Error(message || "Не удалось выполнить действие. Код ответа: " + response.status + ".");
        error.status = response.status;
        error.code = value.error && value.error.code;
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
    if (snapshot) updateControls();
  }

  async function refreshState() {
    if (!session.authenticated) return;
    const value = await api("/api/state");
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
    if (pollInFlight || !session.authenticated) return;
    pollInFlight = true;
    try {
      await refreshState();
      if (activePage === "queue") await loadJobs(false);
    } catch (error) {
      if (session.authenticated) setConnectivity(false, error.message + " Данные на экране могут быть устаревшими.");
    } finally {
      pollInFlight = false;
      clearTimeout(pollTimer);
      if (session.authenticated) pollTimer = setTimeout(poll, document.hidden ? 5000 : 2000);
    }
  }

  function phase() { return plain(snapshot && snapshot.activity && snapshot.activity.phase, "idle"); }
  function isBusy() { return busyPhases.has(phase()); }
  function anyAction() { return pendingActions.size > 0 || pendingTaskActions.size > 0; }
  function canControlTasks() { return Boolean(snapshot && snapshot.task_controls_available === true && connected && session.authenticated && pendingActions.size === 0); }
  function canClearQueue() { return Boolean(snapshot && snapshot.queue_clear_available === true && connected && session.authenticated && !anyAction()); }

  function updateControls() {
    if (!snapshot) return;
    const hasSources = Array.isArray(snapshot.sources) && snapshot.sources.length > 0;
    const busy = isBusy();
    const transient = anyAction();
    const current = phase();
    $("start-upload").disabled = busy || transient || !snapshot.authenticated || !hasSources || !connected;
    $("scan-folders").disabled = busy || transient || !hasSources || !connected;
    const pausable = ["scanning", "checking", "probing", "uploading", "pausing", "reconciling"].includes(current);
    setHidden("pause-upload", !pausable);
    setHidden("start-upload", pausable);
    $("pause-upload").disabled = current === "pausing" || pendingActions.has("pause") || !connected;
    $("pause-upload").querySelector("span").textContent = current === "scanning" ? "Остановить сканирование" : current === "pausing" ? "Сохраняем прогресс…" : "Приостановить";
    const resumed = ["paused", "error"].includes(current) || finite(snapshot.stats && snapshot.stats.uploaded) > 0;
    $("start-upload").querySelector("span").textContent = resumed ? "Продолжить загрузку" : "Начать загрузку";
    const sourcesDisabled = busy || transient || !connected;
    ["add-source", "empty-add-source", "onboarding-source", "source-submit", "source-remove-submit", "browse-folders", "source-path", "source-alias", "source-remote-prefix"].forEach((id) => { $(id).disabled = sourcesDisabled; });
    document.querySelectorAll("[data-source-action]").forEach((button) => { button.disabled = sourcesDisabled; });
    setHidden("sources-busy-note", !busy);
    setHidden("source-form-busy", !busy);
    setHidden("source-remove-busy", !busy);
    ["onboarding-login", "settings-login", "account-menu-login"].forEach((id) => { $(id).disabled = busy || transient || !connected; });
    $("account-button").disabled = ["login", "logout"].includes(current) || transient || !connected;
    $("account-menu-logout").disabled = !snapshot.authenticated || current === "logout" || transient || !connected;
    setText("account-menu-note", busy ? "Выход остановит текущую работу и сохранит очередь." : "Очередь и завершённые задачи сохранятся. Для продолжения войдите в этот же аккаунт.");
    $("retry-errors").disabled = busy || transient || finite(snapshot.stats && snapshot.stats.failed) === 0 || !connected;
    $("save-settings").disabled = busy || transient || !connected;
    $("workers").disabled = busy || transient;
    $("auto-resume").disabled = busy || transient;
    $("preflight-check").disabled = busy || transient;
    const canClear = canClearQueue();
    $("clear-queue").disabled = !canClear;
    $("queue-clear-submit").disabled = !canClear;
    $("queue-clear-options").disabled = !canClear;
    setHidden("queue-clear-busy", snapshot.queue_clear_available === true || !connected);
    setText("queue-clear-submit", pendingActions.has("clear") ? "Очищаем…" : $("queue-clear-all").checked ? "Очистить всю очередь" : "Убрать обработанные");
    setHidden("task-controls-note", snapshot.task_controls_available === true || !connected || jobsTotal === 0);
    jobViews.forEach(updateJobControls);
  }

  function renderState() {
    const state = snapshot || {};
    renderConnectivity();
    const stats = state.stats || {};
    const sources = Array.isArray(state.sources) ? state.sources : [];
    const activity = state.activity || {};
    const current = phase();
    const info = phaseInfo[current] || phaseInfo.idle;
    const total = Math.max(0, finite(stats.total));
    const uploaded = Math.max(0, finite(stats.uploaded));
    const failed = Math.max(0, finite(stats.failed)), skipped = Math.max(0, finite(stats.skipped));
    const finishedErrors = Math.max(0, finite(stats.permanent_failures ?? failed));
    const processed = Math.min(total, uploaded + finishedErrors + skipped);
    const percentage = total > 0 ? processed / total * 100 : 0;
    setText("version-label", "Версия " + plain(state.version, "0.4"));
    setText("account-label", state.authenticated ? plain(state.account, "Вход сохранён") : "Подключить Mail.ru");
    $("account-button").title = state.authenticated ? "Вход в Mail.ru сохранён: " + plain(state.account) : "Подключить аккаунт Mail.ru";
    $("account-button").setAttribute("aria-haspopup", state.authenticated ? "menu" : "dialog");
    setText("account-menu-email", plain(state.account, "Аккаунт Mail.ru"));
    if (!state.authenticated) closeAccountMenu();
    setText("nav-queue-count", count(finite(stats.pending) + finite(stats.running)));
    setText("transfer-title", info[0]);
    setText("phase-badge", info[1]);
    $("phase-badge").className = "badge " + info[2];
    $("phase-icon").replaceChildren(icon(info[3]));
    let fallbackMessage = !state.authenticated ? "Подключите аккаунт Mail.ru." : sources.length === 0 ? "Добавьте исходную папку с фото и видео." : total === 0 ? "Нажмите «Начать загрузку»: клиент сам найдёт фото и видео." : "Прогресс задач сохранён. Можно продолжить работу.";
    setText("activity-message", plain(activity.message, fallbackMessage) || fallbackMessage);
    setText("processed-count", count(processed));
    setText("total-count", count(total));
    setText("progress-percent", percentage.toLocaleString("ru-RU", { maximumFractionDigits: 1 }) + "%");
    $("overall-progress-fill").style.width = percentage + "%";
    $("overall-progress").setAttribute("aria-valuenow", percentage.toFixed(1));
    $("overall-progress").setAttribute("aria-valuetext", "Обработано задач: " + count(processed) + " из " + count(total));
    setText("transfer-bytes", "Загружено: " + bytes(stats.uploaded_bytes) + " · Объём задач: " + bytes(stats.total_bytes));
    setText("transfer-outcomes", "Успешно: " + count(uploaded) + " · Ошибок: " + count(failed) + " · Пропущено: " + count(skipped));
    setText("stat-pending", count(finite(stats.pending) + finite(stats.running)));
    const byStatus = stats.bytes_by_status || {};
    setText("stat-pending-bytes", bytes(finite(byStatus.pending) + finite(byStatus.running)));
    setText("stat-uploaded", count(uploaded));
    setText("stat-uploaded-bytes", bytes(stats.uploaded_bytes));
    setText("stat-failed", count(failed));
    setText("stat-sources", count(sources.length));
    setText("stat-skipped", "Пропущено: " + count(skipped));
    const scan = activity.scan || {};
    setHidden("scan-live", current !== "scanning");
    setText("scan-seen", count(scan.seen));
    setText("scan-added", count(scan.added));
    setText("scan-changed", count(scan.changed));
    setHidden("probe-live", current !== "probing" && current !== "checking");
    setText("probe-message", current === "checking" ? "Проверяем аккаунт и доступность безлимита…" : "Контрольная проверка безлимита выполняется автоматически.");
    renderProbeCountdown();
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
    updateControls();
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

  function renderProbeCountdown() {
    const until = finite(snapshot && snapshot.activity && snapshot.activity.probe_wait_until);
    const seconds = until > 0 ? Math.max(0, Math.ceil(until - Date.now() / 1000)) : 0;
    setText("probe-countdown", seconds > 0 ? seconds + " с" : "");
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
    setText("settings-account", state.authenticated ? plain(state.account, "Вход сохранён") : "Аккаунт не подключён");
    setText("settings-login", state.authenticated ? "Войти заново" : "Войти");
    if (!settingsDirty && !pendingActions.has("settings")) {
      $("workers").value = String(finite(state.settings && state.settings.workers, 2));
      $("auto-resume").checked = !state.settings || state.settings.auto_resume !== false;
      $("preflight-check").checked = !state.settings || state.settings.preflight_check !== false;
    }
  }

  function navigate(page) {
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
  }

  async function loadJobs(showLoading = true) {
    if (!session.authenticated) return;
    const request = ++jobsRequest;
    const offset = jobsOffset, filter = jobsFilter;
    if (showLoading) setText("jobs-loading", "Обновляем…");
    try {
      const query = new URLSearchParams({ status: filter, offset: String(offset), limit: String(jobsLimit) });
      const result = await api("/api/jobs?" + query.toString());
      if (request !== jobsRequest || offset !== jobsOffset || filter !== jobsFilter) return;
      jobsTotal = Math.max(0, finite(result.total));
      if (jobsOffset >= jobsTotal && jobsOffset > 0) {
        jobsOffset = Math.max(0, Math.floor(Math.max(0, jobsTotal - 1) / jobsLimit) * jobsLimit);
        return loadJobs(showLoading);
      }
      renderJobs(Array.isArray(result.jobs) ? result.jobs : []);
      setText("jobs-loading", "");
    } catch (error) {
      if (request === jobsRequest) setText("jobs-loading", error.message);
    }
  }

  function taskPrimaryAction(job) {
    if (["uploaded", "skipped"].includes(job.status) || job.remove_requested === true) return null;
    if (job.paused === true) return job.status === "running" ? ["pause", "Пауза…", "pause"] : ["resume", "Продолжить", "play"];
    if (job.status === "failed") return ["resume", "Повторить", "retry"];
    return ["pending", "running"].includes(job.status) ? ["pause", "Пауза", "pause"] : null;
  }

  function taskState(job) {
    if (job.remove_requested === true) return ["Убирается…", "neutral"];
    if (job.paused === true && job.status === "running") return ["Останавливается…", "orange"];
    if (job.paused === true && !["uploaded", "skipped"].includes(job.status)) return jobStates.paused;
    if (job.status === "failed" && job.next_attempt != null) return ["Повтор позже", "orange"];
    return jobStates[job.status] || ["Неизвестно", "neutral"];
  }

  function createJobView(job) {
    const row = element("tr", "task-row"); row.dataset.taskId = String(job.id);
    const nameCell = element("td", "task-name-cell");
    const name = element("div", "job-name"), fileIcon = icon("image"), label = element("span");
    name.append(fileIcon, label);
    const remote = element("div", "job-path");
    const storage = element("span", "storage-mode", "Обычное хранение");
    storage.title = "Режим загрузки с расходованием места по тарифу";
    const detail = element("div", "job-detail"), actionError = element("div", "task-action-error");
    actionError.setAttribute("role", "status"); actionError.hidden = true;
    nameCell.append(name, remote, storage, detail, actionError);
    const statusCell = element("td", "task-status-cell"), badge = element("span", "badge neutral");
    const progress = element("div", "task-progress"), progressHeading = element("div", "task-progress-heading");
    const stage = element("span"), percentage = element("span", "task-percent");
    progressHeading.append(stage, percentage);
    const track = element("div", "progress-track"), fill = element("div", "progress-fill"), transferred = element("div", "task-transferred");
    track.setAttribute("role", "progressbar"); track.setAttribute("aria-valuemin", "0"); track.setAttribute("aria-valuemax", "100");
    track.appendChild(fill); progress.append(progressHeading, track, transferred); statusCell.append(badge, progress);
    const size = element("td", "task-size"), attempts = element("td", "task-attempts"), actionsCell = element("td", "task-actions-cell"), actions = element("div", "task-actions");
    const primary = element("button", "button compact task-primary"), primaryIcon = icon("pause", "small"), primaryLabel = element("span");
    primary.type = "button"; primary.dataset.taskControl = "primary"; primary.append(primaryIcon, primaryLabel);
    const remove = element("button", "button compact danger subtle task-remove"); remove.type = "button"; remove.dataset.taskControl = "remove"; remove.appendChild(icon("remove", "small"));
    actions.append(primary, remove); actionsCell.appendChild(actions); row.append(nameCell, statusCell, size, attempts, actionsCell);
    const view = { row, name, fileIcon, label, remote, storage, detail, actionError, badge, progress, stage, percentage, track, fill, transferred, size, attempts, actions, primary, primaryIcon, primaryLabel, remove, job, error: "" };
    primary.addEventListener("click", () => {
      const action = taskPrimaryAction(view.job);
      if (action && primary.getAttribute("aria-disabled") !== "true") performTask(view, action[0]);
    });
    remove.addEventListener("click", () => { if (remove.getAttribute("aria-disabled") !== "true") performTask(view, "remove"); });
    return view;
  }

  function updateJobControls(view) {
    const job = view.job, pending = pendingTaskActions.get(job.id);
    const action = taskPrimaryAction(job), allowed = canControlTasks() && !pending && job.remove_requested !== true;
    const pausing = job.paused === true && job.status === "running";
    const name = plain(job.relative_path, plain(job.local_path, "Файл"));
    view.primary.hidden = !action;
    if (action) {
      view.primaryIcon.firstElementChild.setAttribute("href", "#i-" + action[2]);
      view.primaryLabel.textContent = pending === "pause" ? "Пауза…" : pending === "resume" ? "Возврат…" : action[1];
      view.primary.setAttribute("aria-label", action[1] + ": " + name);
      view.primary.setAttribute("aria-disabled", String(!allowed || pausing));
      view.primary.title = pausing ? "Дождитесь сохранения прогресса этой задачи." : action[0] === "resume" ? "Вернуть задачу в очередь. Общая пауза сохраняется." : "Приостановить только эту задачу.";
    }
    view.remove.setAttribute("aria-label", "Убрать задачу: " + name);
    view.remove.setAttribute("aria-disabled", String(!allowed));
    view.remove.title = "Убрать задачу из очереди. Файлы на сервере и в Облаке останутся.";
    view.actions.setAttribute("aria-busy", String(Boolean(pending)));
    view.actionError.textContent = view.error;
    view.actionError.hidden = !view.error;
  }

  function updateJobView(view, job) {
    view.job = job;
    const fullName = plain(job.relative_path, plain(job.local_path, "Файл"));
    view.label.textContent = fullName;
    view.fileIcon.firstElementChild.setAttribute("href", "#i-" + mediaIcon(fullName));
    view.name.title = plain(job.local_path, fullName);
    view.remote.textContent = plain(job.remote_path); view.remote.title = plain(job.remote_path);
    view.storage.hidden = job.storage_mode !== "regular";
    const state = taskState(job);
    view.badge.textContent = state[0]; view.badge.className = "badge " + state[1];
    let detail = plain(job.last_error);
    if (job.next_attempt != null && job.status === "failed") detail += (detail ? " · " : "") + "Автоматический повтор: " + dateTime(job.next_attempt);
    if (!detail && job.status === "uploaded") detail = job.uploaded_at ? "Загружено: " + dateTime(job.uploaded_at) : "Файл сохранён в Облаке";
    if (!detail && job.status === "skipped") detail = "Задача пропущена. Подробности — в журнале работы.";
    view.detail.textContent = detail; view.detail.hidden = !detail;
    view.detail.className = "job-detail" + (job.status === "failed" ? " failed" : "");
    view.size.textContent = bytes(job.size); view.attempts.textContent = count(job.attempts);
    const progress = job.progress && typeof job.progress === "object" ? job.progress : null;
    const completed = job.status === "uploaded";
    const hasProgress = completed || progress !== null;
    view.progress.hidden = !hasProgress;
    if (hasProgress) {
      const total = completed ? Math.max(0, finite(job.size)) : Math.max(0, finite(progress.total, finite(job.size)));
      const sent = completed ? total : Math.max(0, Math.min(total, finite(progress.sent)));
      const percent = completed ? 100 : Math.max(0, Math.min(100, finite(progress.percent, total > 0 ? sent / total * 100 : 0)));
      const stage = completed ? "Готово" : transferStages[plain(progress.stage)] || "Обработка";
      const label = percent.toLocaleString("ru-RU", { maximumFractionDigits: 1 }) + "%";
      view.stage.textContent = stage; view.percentage.textContent = label;
      view.track.setAttribute("aria-label", "Передача: " + fullName);
      view.track.setAttribute("aria-valuenow", percent.toFixed(1));
      view.track.setAttribute("aria-valuetext", stage + ": " + label);
      view.fill.style.width = percent + "%";
      view.progress.classList.toggle("complete", completed);
      view.transferred.textContent = bytes(sent) + " / " + bytes(total);
    }
    updateJobControls(view);
  }

  function restoreTaskFocus(focused, views) {
    if (!focused) return;
    const current = document.activeElement;
    if (current === focused.element && current.isConnected && !current.hidden) return;
    if (current && current !== document.body && current !== focused.element && current.isConnected) return;
    const view = jobViews.get(focused.id) || views[Math.min(focused.index, views.length - 1)];
    const target = view ? focused.control === "remove" || view.primary.hidden ? view.remove : view.primary : $("queue-title");
    target.focus({ preventScroll: true });
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
    restoreTaskFocus(focused, views);
  }

  async function performTask(view, action) {
    const job = view.job;
    if (!canControlTasks() || pendingTaskActions.has(job.id) || job.remove_requested === true) return;
    if (action !== "remove" && (!taskPrimaryAction(job) || taskPrimaryAction(job)[0] !== action)) return;
    if (action === "pause" && job.paused === true) return;
    pendingTaskActions.set(job.id, action); view.error = ""; updateControls();
    let accepted = false;
    try {
      await api("/api/tasks/" + action, { method: "POST", body: { id: job.id } });
      accepted = true;
      await refreshState();
      if (activePage === "queue") await loadJobs(false);
      if (action === "remove") toast("Убираем задачу из очереди. Файлы на сервере и в Облаке остаются.");
      else if (action === "pause") toast("Запрос на паузу выбранной задачи принят.");
      else if (phase() === "paused") toast("Задача возвращена в очередь. Общая пауза сохранена — нажмите «Продолжить загрузку», когда будете готовы.");
      else if (snapshot && snapshot.queue_clear_available === true) toast("Задача возвращена в очередь. Запустите загрузку на странице «Обзор».");
      else toast("Задача возвращена в очередь.");
    } catch (error) {
      view.error = accepted ? "Команда принята, но состояние не удалось обновить. " + error.message : error.message;
      toast(view.error, true);
    } finally {
      pendingTaskActions.delete(job.id); updateControls();
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
    } finally { pendingActions.delete(key); updateControls(); }
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
  $("mail-dialog").addEventListener("close", () => { $("mail-password").value = ""; });
  window.addEventListener("hashchange", () => navigate(location.hash.slice(1)));
  document.addEventListener("visibilitychange", () => { if (!document.hidden && session.authenticated) { clearTimeout(pollTimer); poll(); } });

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
    try {
      await api("/api/session/logout", { method: "POST" });
      session.authenticated = false; snapshot = null;
      await refreshSession(); setError("access-error");
    } catch (error) { toast(error.message, true); }
  }
  function accountMenuItems() { return [...$("account-menu").querySelectorAll('[role="menuitem"]')].filter((item) => !item.disabled); }
  function closeAccountMenu(restoreFocus = false) {
    const wasOpen = !$("account-menu").hidden;
    setHidden("account-menu", true);
    $("account-button").setAttribute("aria-expanded", "false");
    if (wasOpen && restoreFocus) $("account-button").focus();
  }
  function openAccountMenu(last = false) {
    if (!snapshot || !snapshot.authenticated) { openMail(); return; }
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
    if (["ArrowDown", "ArrowUp"].includes(event.key) && snapshot && snapshot.authenticated) {
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
  $("start-upload").addEventListener("click", () => perform("start", "/api/start"));
  $("pause-upload").addEventListener("click", () => perform("pause", "/api/pause", {}, { message: "Останавливаем текущую работу и сохраняем прогресс…" }));
  $("scan-folders").addEventListener("click", () => perform("scan", "/api/scan"));
  $("view-errors").addEventListener("click", () => { setFilter("failed"); location.hash = "queue"; if (activePage === "queue") loadJobs(); });
  ["settings-login", "onboarding-login"].forEach((id) => $(id).addEventListener("click", openMail));
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
    jobsFilter = Object.prototype.hasOwnProperty.call(jobStates, filter) ? filter : "all";
    jobsOffset = 0;
    document.querySelectorAll(".filter-tab").forEach((button) => { const active = button.dataset.filter === jobsFilter; button.classList.toggle("active", active); button.setAttribute("aria-pressed", String(active)); });
  }
  document.querySelectorAll(".filter-tab").forEach((button) => button.addEventListener("click", () => { setFilter(button.dataset.filter); loadJobs(); }));
  $("jobs-prev").addEventListener("click", () => { jobsOffset = Math.max(0, jobsOffset - jobsLimit); loadJobs(); });
  $("jobs-next").addEventListener("click", () => { if (jobsOffset + jobsLimit < jobsTotal) { jobsOffset += jobsLimit; loadJobs(); } });
  $("retry-errors").addEventListener("click", () => { $("retry-permanent").checked = false; setError("retry-error"); showDialog("retry-dialog"); });
  $("retry-form").addEventListener("submit", async (event) => {
    event.preventDefault(); $("retry-submit").disabled = true;
    try { await perform("retry", "/api/retry", { include_permanent: $("retry-permanent").checked }, { errorId: "retry-error", close: "retry-dialog", message: "Подходящие задачи возвращены в очередь." }); }
    finally { $("retry-submit").disabled = false; }
  });
  $("clear-queue").addEventListener("click", () => {
    if (!canClearQueue()) return;
    $("queue-clear-form").reset();
    setError("queue-clear-error"); updateControls(); showDialog("queue-clear-dialog");
  });
  ["queue-clear-finished", "queue-clear-all"].forEach((id) => $(id).addEventListener("change", updateControls));
  $("queue-clear-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!canClearQueue()) return;
    const scope = $("queue-clear-all").checked ? "all" : "finished";
    const message = scope === "all" ? "Очередь очищена. Источники и файлы сохранены. Новые задачи появятся после ручного сканирования." : "Обработанные задачи убраны. Ожидающие загрузки и автоматического повтора сохранены.";
    if (await perform("clear", "/api/tasks/clear", { scope }, { errorId: "queue-clear-error", close: "queue-clear-dialog", message, resetJobs: true })) {
      $("queue-title").focus({ preventScroll: true });
    }
  });
  ["workers", "auto-resume", "preflight-check"].forEach((id) => $(id).addEventListener("input", () => { settingsDirty = true; }));
  $("settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const workers = Number($("workers").value);
    if (!Number.isInteger(workers) || workers < 1 || workers > 16) { setError("settings-error", "Укажите целое число потоков от 1 до 16."); return; }
    const success = await perform("settings", "/api/settings", { workers, auto_resume: $("auto-resume").checked, preflight_check: $("preflight-check").checked }, { errorId: "settings-error", message: "Настройки сохранены." });
    if (success) { settingsDirty = false; if (snapshot) renderSettings(snapshot); }
  });

  setInterval(renderProbeCountdown, 1000);
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
