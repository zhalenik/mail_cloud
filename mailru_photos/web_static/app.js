"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const numberFormat = new Intl.NumberFormat("ru-RU");
  const pageNames = { overview: "Обзор", queue: "Очередь файлов", sources: "Исходные папки", settings: "Настройки" };
  const busyPhases = new Set(["login", "logout", "scanning", "checking", "probing", "uploading", "pausing"]);
  const phaseInfo = {
    idle: ["Готов к работе", "Ожидание", "neutral", "cloud"],
    login: ["Подключаем аккаунт", "Подключение", "blue", "lock"],
    logout: ["Выходим из Mail.ru", "Выход", "orange", "logout"],
    scanning: ["Ищем фото и видео", "Сканирование", "blue", "scan"],
    checking: ["Проверяем доступ к Облаку", "Проверка", "blue", "lock"],
    probing: ["Проверяем безлимит", "Проверка", "blue", "lock"],
    uploading: ["Загружаем файлы", "Загрузка", "blue", "cloud"],
    pausing: ["Сохраняем прогресс", "Останавливаем", "orange", "pause"],
    paused: ["Загрузка приостановлена", "На паузе", "orange", "pause"],
    completed: ["Обработка завершена", "Завершено", "green", "check"],
    error: ["Нужно ваше внимание", "Приостановлено", "red", "alert"]
  };
  const jobStates = {
    pending: ["Ожидает", "neutral"], running: ["В работе", "blue"], uploaded: ["Загружен", "green"],
    failed: ["Ошибка", "red"], skipped: ["Пропущен", "orange"]
  };
  let session = { authenticated: false, setup_required: false, csrf: "" };
  let snapshot = null;
  let activePage = "overview";
  let pollTimer = null;
  let pollInFlight = false;
  let pollCount = 0;
  let toastTimer = null;
  let jobsFilter = "all";
  let jobsOffset = 0;
  let jobsTotal = 0;
  let jobsRequest = 0;
  let folderState = { path: null, parent: null };
  let folderRequest = 0;
  let settingsDirty = false;
  let connected = true;
  let accessBusy = false;
  let mailLogoutRequested = false;
  const pendingActions = new Set();
  const jobsLimit = 50;

  function finite(value, fallback = 0) {
    return typeof value === "number" && Number.isFinite(value) ? value : fallback;
  }
  function count(value) { return numberFormat.format(Math.max(0, Math.floor(finite(value)))); }
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
      pollCount += 1;
      if (activePage === "queue" && pollCount % 2 === 0) await loadJobs(false);
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
  function anyAction() { return pendingActions.size > 0; }

  function updateControls() {
    if (!snapshot) return;
    const hasSources = Array.isArray(snapshot.sources) && snapshot.sources.length > 0;
    const configured = snapshot.configured === true;
    const busy = isBusy();
    const transient = anyAction();
    const current = phase();
    $("start-upload").disabled = busy || transient || !configured || !snapshot.authenticated || !hasSources || !connected;
    $("scan-folders").disabled = busy || transient || !configured || !hasSources || !connected;
    const pausable = ["scanning", "checking", "probing", "uploading", "pausing"].includes(current);
    setHidden("pause-upload", !pausable);
    setHidden("start-upload", pausable);
    $("pause-upload").disabled = current === "pausing" || pendingActions.has("pause") || !connected;
    $("pause-upload").querySelector("span").textContent = current === "scanning" ? "Остановить сканирование" : current === "pausing" ? "Сохраняем прогресс…" : "Приостановить";
    const resumed = ["paused", "error"].includes(current) || finite(snapshot.stats && snapshot.stats.uploaded) > 0;
    $("start-upload").querySelector("span").textContent = resumed ? "Продолжить загрузку" : "Начать загрузку";
    ["add-source", "empty-add-source", "onboarding-source"].forEach((id) => { $(id).disabled = busy || transient || !configured || !connected; });
    ["onboarding-login", "settings-login", "account-menu-login"].forEach((id) => { $(id).disabled = busy || transient || !configured || !connected; });
    $("account-button").disabled = ["login", "logout"].includes(current) || transient || !configured || !connected;
    $("account-menu-logout").disabled = !snapshot.authenticated || current === "logout" || transient || !connected;
    setText("account-menu-note", busy ? "Выход остановит текущую работу и сохранит очередь." : "Очередь и завершённые задания сохранятся. Для продолжения войдите в этот же аккаунт.");
    $("open-setup").disabled = configured || busy || transient || !connected;
    $("settings-setup").disabled = configured || busy || transient || !connected;
    setHidden("settings-setup", configured);
    $("retry-errors").disabled = busy || transient || !configured || finite(snapshot.stats && snapshot.stats.failed) === 0 || !connected;
    $("save-settings").disabled = busy || transient || !connected;
    $("workers").disabled = busy || transient;
    $("auto-resume").disabled = busy || transient;
    $("preflight-check").disabled = busy || transient;
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
    const percentage = total > 0 ? Math.min(100, uploaded / total * 100) : 0;
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
    let fallbackMessage = !state.configured ? "Выберите папку назначения в Облаке." : !state.authenticated ? "Подключите аккаунт Mail.ru." : sources.length === 0 ? "Добавьте исходную папку с фото и видео." : total === 0 ? "Нажмите «Начать загрузку»: клиент сам найдёт фото и видео." : "История сохранена. Можно продолжить работу.";
    setText("activity-message", plain(activity.message, fallbackMessage) || fallbackMessage);
    setText("uploaded-count", count(uploaded));
    setText("total-count", count(total));
    setText("progress-percent", percentage.toLocaleString("ru-RU", { maximumFractionDigits: 1 }) + "%");
    $("overall-progress-fill").style.width = percentage + "%";
    $("overall-progress").setAttribute("aria-valuenow", percentage.toFixed(1));
    $("overall-progress").setAttribute("aria-valuetext", count(uploaded) + " из " + count(total) + " файлов загружено");
    setText("transfer-bytes", bytes(stats.uploaded_bytes) + " из " + bytes(stats.total_bytes));
    setText("destination-label", state.destination ? "В Облаке: " + state.destination : "Назначение ещё не выбрано");
    $("destination-label").title = plain(state.destination);
    setText("stat-pending", count(finite(stats.pending) + finite(stats.running)));
    const byStatus = stats.bytes_by_status || {};
    setText("stat-pending-bytes", bytes(finite(byStatus.pending) + finite(byStatus.running)));
    setText("stat-uploaded", count(uploaded));
    setText("stat-uploaded-bytes", bytes(stats.uploaded_bytes));
    setText("stat-failed", count(stats.failed));
    setText("stat-sources", count(sources.length));
    setText("stat-skipped", "Пропущено: " + count(stats.skipped));
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
    const completed = [state.configured === true, state.authenticated === true, sources.length > 0];
    setHidden("onboarding", completed.every(Boolean));
    setText("setup-count", completed.filter(Boolean).length + " из 3");
    ["destination", "account", "sources"].forEach((name, index) => {
      const node = $("step-" + name);
      node.classList.toggle("done", completed[index]);
      const number = node.querySelector(".step-number");
      if (completed[index]) { number.replaceChildren(icon("check")); number.firstChild.style.width = "13px"; number.firstChild.style.height = "13px"; }
      else number.textContent = String(index + 1);
      node.querySelector("button").hidden = completed[index];
    });
    setText("step-destination-text", completed[0] ? state.destination : "Где хранить ваш архив");
    setText("step-account-text", completed[1] ? plain(state.account, "Вход сохранён") : "Войдите с паролем приложения");
    setText("step-sources-text", completed[2] ? "Подключено папок: " + count(sources.length) : "Папки на вашем сервере");
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
    setText("active-count", count(files.length) + " файлов");
    if (!files.length) {
      let title = "Активных загрузок пока нет", message = "Здесь появятся файлы, которые обрабатываются прямо сейчас.";
      if (phase() === "paused") { title = "Очередь на паузе"; message = "Нажмите «Продолжить загрузку», когда будете готовы."; }
      if (phase() === "completed") { title = "Все текущие задания обработаны"; message = "Добавьте новые фото или видео и нажмите «Найти новые файлы»."; }
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
      const stageNames = { hash: "Проверка файла", hashing: "Проверка файла", upload: "Передача", uploading: "Передача", blob_upload: "Передача", stat: "Проверка", verify: "Проверка", verifying: "Проверка", commit: "Подтверждение", add: "Подтверждение", unlimited_add: "Подтверждение", preparing: "Подготовка" };
      const item = element("div", "active-file");
      const heading = element("div", "active-file-heading");
      const fileIcon = element("span", "file-icon"); fileIcon.appendChild(icon(mediaIcon(name)));
      const label = element("div");
      const title = element("div", "active-file-name", name); title.title = fullPath;
      const path = element("div", "active-file-path", fullPath); path.title = fullPath;
      label.append(title, path);
      appendStorageMode(label, file.storage_mode);
      heading.append(fileIcon, label, element("span", "badge blue", stageNames[stage] || "Обработка"));
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
    const list = $("sources-list"); list.replaceChildren();
    sources.forEach((source) => {
      const card = element("article", "card source-card");
      const symbol = element("span", "stat-icon blue"); symbol.appendChild(icon("folder"));
      const info = element("div", "source-info");
      info.appendChild(element("h2", "", plain(source.alias, "Папка")));
      const local = element("div", "source-path-line"), remote = element("div", "source-path-line");
      local.append(element("span", "", "На сервере"), element("span", "", plain(source.path)));
      remote.append(element("span", "", "В Облаке"), element("span", "", plain(source.remote_prefix)));
      info.append(local, remote);
      card.append(symbol, info, element("span", "badge neutral", "Все подпапки"));
      list.appendChild(card);
    });
  }

  function renderSettings(state) {
    setText("settings-account", state.authenticated ? plain(state.account, "Вход сохранён") : "Аккаунт не подключён");
    setText("settings-login", state.authenticated ? "Войти заново" : "Войти");
    setText("settings-destination", plain(state.destination, "Ещё не выбрана"));
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

  function renderJobs(jobs) {
    const body = $("jobs-body"); body.replaceChildren();
    setHidden("jobs-empty", jobs.length > 0);
    setText("jobs-empty-title", jobsFilter === "all" ? "В очереди пока нет файлов" : "Таких заданий сейчас нет");
    setText("jobs-empty-description", jobsFilter === "all" ? "Добавьте исходную папку и запустите сканирование." : "Выберите другой фильтр, чтобы посмотреть остальные файлы.");
    jobs.forEach((job) => {
      const row = document.createElement("tr");
      const nameCell = document.createElement("td");
      const name = element("div", "job-name");
      const fullName = plain(job.relative_path, plain(job.local_path, "Файл"));
      name.append(icon(mediaIcon(fullName)), element("span", "", fullName));
      name.title = plain(job.local_path, fullName);
      const remote = element("div", "job-path", plain(job.remote_path)); remote.title = plain(job.remote_path);
      nameCell.append(name, remote);
      appendStorageMode(nameCell, job.storage_mode);
      const statusCell = document.createElement("td");
      const state = jobStates[job.status] || ["Неизвестно", "neutral"];
      statusCell.appendChild(element("span", "badge " + state[1], state[0]));
      const detailCell = element("td", "job-detail" + (job.status === "failed" ? " failed" : ""));
      let detail = plain(job.last_error);
      if (!detail && job.status === "uploaded") detail = dateTime(job.uploaded_at) || "Сохранён в Облаке";
      if (!detail && job.next_attempt) detail = "Повтор: " + dateTime(job.next_attempt);
      if (!detail && job.status === "skipped") detail = "Смотрите журнал работы";
      detailCell.textContent = detail || "—";
      row.append(nameCell, statusCell, element("td", "", bytes(job.size)), element("td", "", count(job.attempts)), detailCell);
      body.appendChild(row);
    });
    setText("jobs-range", jobsTotal === 0 ? "0 файлов" : count(jobsOffset + 1) + "–" + count(jobsOffset + jobs.length) + " из " + count(jobsTotal));
    $("jobs-prev").disabled = jobsOffset === 0;
    $("jobs-next").disabled = jobsOffset + jobs.length >= jobsTotal;
  }

  async function perform(key, path, body = {}, options = {}) {
    if (pendingActions.has(key)) return false;
    pendingActions.add(key); updateControls();
    if (options.errorId) setError(options.errorId);
    try {
      await api(path, { method: "POST", body });
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

  function openSetup() {
    if (snapshot && snapshot.configured) return;
    setError("setup-error"); showDialog("setup-dialog");
  }
  function openMail() {
    closeAccountMenu();
    if (!snapshot || !snapshot.configured) { openSetup(); return; }
    $("mail-email").value = plain(snapshot.account);
    $("mail-password").value = "";
    setError("mail-error"); showDialog("mail-dialog");
  }
  function openSource() {
    if (!snapshot || !snapshot.configured) { openSetup(); return; }
    $("source-form").reset();
    setError("source-error"); updateSourcePreview(); showDialog("source-dialog");
  }
  function updateSourcePreview() {
    const destination = plain(snapshot && snapshot.destination, "/PhotoArchivePC").replace(/\/$/, "");
    const alias = $("source-alias").value.trim();
    setText("source-preview", alias ? "В Облаке: " + destination + "/" + alias : "Это имя подпапки внутри вашего фотоархива.");
  }
  async function openFolderPicker() {
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
  ["open-setup", "settings-setup"].forEach((id) => $(id).addEventListener("click", openSetup));
  ["settings-login", "onboarding-login"].forEach((id) => $(id).addEventListener("click", openMail));
  ["add-source", "empty-add-source", "onboarding-source"].forEach((id) => $(id).addEventListener("click", openSource));

  $("setup-form").addEventListener("submit", async (event) => {
    event.preventDefault(); $("setup-submit").disabled = true;
    try { await perform("setup", "/api/setup", { destination: $("destination-input").value.trim() }, { errorId: "setup-error", close: "setup-dialog", message: "Папка назначения сохранена. Теперь подключите Mail.ru." }); }
    finally { $("setup-submit").disabled = false; }
  });
  $("mail-form").addEventListener("submit", async (event) => {
    event.preventDefault(); if (pendingActions.has("login")) return;
    $("mail-submit").disabled = true;
    try {
      const request = perform("login", "/api/login", { email: $("mail-email").value.trim(), password: $("mail-password").value }, { errorId: "mail-error", close: "mail-dialog", message: "Проверяем вход в Облако…" });
      $("mail-password").value = "";
      await request;
    } finally { $("mail-submit").disabled = false; }
  });
  $("source-alias").addEventListener("input", updateSourcePreview);
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
    event.preventDefault(); $("source-submit").disabled = true;
    try { await perform("source", "/api/sources", { path: $("source-path").value.trim(), alias: $("source-alias").value.trim() }, { errorId: "source-error", close: "source-dialog", message: "Папка добавлена. Нажмите «Начать загрузку» или запустите сканирование." }); }
    finally { $("source-submit").disabled = false; }
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
    try { await perform("retry", "/api/retry", { include_permanent: $("retry-permanent").checked }, { errorId: "retry-error", close: "retry-dialog", message: "Подходящие задания возвращены в очередь." }); }
    finally { $("retry-submit").disabled = false; }
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
