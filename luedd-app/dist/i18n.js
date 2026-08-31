// Tiny i18n for the Lüdd windows. No framework: a flat key -> string map per
// language, a `t()` lookup with English fallback, and `applyLang()` which walks
// [data-i18n] / [data-i18n-placeholder] elements. Dynamic strings built in JS
// call I18N.t(key) directly.
(function () {
  const STRINGS = {
    en: {
      run_queue: "Run queue",
      settings: "Settings",
      show_detected: "Detected downloads",
      toggle_all: "Show / hide all details",
      refresh: "Refresh",
      remove_selected: "Remove selected",
      retry_selected: "Retry selected",
      clear_finished: "Clear finished",
      add: "Add",
      filename_ph: "Output filename (optional)",
      col_id: "ID", col_kind: "Kind", col_status: "Status", col_progress: "Progress",
      col_speed: "Speed", col_url: "URL", col_error: "Error", col_actions: "Actions",
      stat_running: "running", stat_queued: "queued",

      settings_title: "Settings",
      sec_downloads: "Downloads",
      sec_appearance: "Appearance",
      output_folder: "Output folder",
      browse: "Browse…",
      max_concurrent: "Max concurrent downloads",
      connections: "Connections per download",
      language: "Language",
      font: "Font",
      font_preview: "The quick brown fox jumps over 1,234 downloads.",
      cancel: "Cancel",
      save: "Save",
      confirm: "Confirm",
      also_delete_files: "Also delete downloaded files",
      clear_field: "Clear",
      choose_quality: "Choose a quality",
      confirm_remove_one: "Remove this download from the list?",
      confirm_remove_n: "Remove {n} selected download(s) from the list?",
      confirm_clear: "Clear all finished/failed/cancelled downloads from the list?",
      copied: "Copied!",
      details: "Details",
      dbl_copy: "(double-click to copy)",

      status_Queued: "Queued", status_Running: "Running", status_Converting: "Converting",
      status_Finished: "Finished", status_Failed: "Failed", status_Cancelled: "Cancelled",
      status_Paused: "Paused",
      act_pause: "Pause", act_resume: "Resume", act_retry: "Retry", act_preview: "Preview", act_open: "Open File",
      open_folder: "Open folder", remove: "Remove", remux: "remux",

      full_size: "Full size",
      d_site: "Site", d_full_link: "Full link", d_saving_to: "Saving to",
      d_headers: "Headers captured", d_cookie: "Cookie captured", d_added: "Added",
      val_none: "none", val_yes: "yes", val_no: "no", val_unknown: "(unknown)",

      // detection window
      detected: "Detected",
      pin: "Pin", unpin: "Unpin", hide: "Hide",
      pin_title: "Toggle always-on-top", hide_title: "Hide this window",
      filter_ph: "Filter by name or site…",
      clear_items: "Clear items",
      monitoring: "Monitoring", monitoring_title: "Stop / resume browser monitoring",
      nothing_detected: "Nothing detected yet.",
      no_matches: "No matches.",
      choose_quality_c: "Choose a quality:",
      site_c: "Site: ", link_c: "Full link: ",
      added_to_luedd: "Added to Lüdd!",
      link_expired: "Failed – link may have expired",
      cant_reach: "Could not reach Lüdd",
    },
    de: {
      run_queue: "Warteschlange starten",
      settings: "Einstellungen",
      show_detected: "Erkannte Downloads",
      toggle_all: "Alle Details ein-/ausblenden",
      refresh: "Aktualisieren",
      remove_selected: "Auswahl entfernen",
      retry_selected: "Auswahl wiederholen",
      clear_finished: "Fertige löschen",
      add: "Hinzufügen",
      filename_ph: "Dateiname (optional)",
      col_id: "ID", col_kind: "Art", col_status: "Status", col_progress: "Fortschritt",
      col_speed: "Tempo", col_url: "URL", col_error: "Fehler", col_actions: "Aktionen",
      stat_running: "laufend", stat_queued: "wartend",

      settings_title: "Einstellungen",
      sec_downloads: "Downloads",
      sec_appearance: "Darstellung",
      output_folder: "Zielordner",
      browse: "Durchsuchen…",
      max_concurrent: "Gleichzeitige Downloads",
      connections: "Verbindungen pro Download",
      language: "Sprache",
      font: "Schriftart",
      font_preview: "Franz jagt im komplett verwahrlosten Taxi quer durch Bayern.",
      cancel: "Abbrechen",
      save: "Speichern",
      confirm: "Bestätigen",
      also_delete_files: "Heruntergeladene Dateien ebenfalls löschen",
      clear_field: "Leeren",
      choose_quality: "Qualität wählen",
      confirm_remove_one: "Diesen Download aus der Liste entfernen?",
      confirm_remove_n: "{n} ausgewählte(n) Download(s) aus der Liste entfernen?",
      confirm_clear: "Alle fertigen/fehlgeschlagenen/abgebrochenen Downloads aus der Liste entfernen?",
      copied: "Kopiert!",
      details: "Details",
      dbl_copy: "(Doppelklick zum Kopieren)",

      status_Queued: "Wartend", status_Running: "Läuft", status_Converting: "Konvertiert",
      status_Finished: "Fertig", status_Failed: "Fehlgeschlagen", status_Cancelled: "Abgebrochen",
      status_Paused: "Pausiert",
      act_pause: "Pause", act_resume: "Fortsetzen", act_retry: "Wiederholen", act_preview: "Vorschau", act_open: "Datei öffnen",
      open_folder: "Ordner öffnen", remove: "Entfernen", remux: "Remux",

      full_size: "Vollansicht",
      d_site: "Seite", d_full_link: "Vollständiger Link", d_saving_to: "Speicherort",
      d_headers: "Erfasste Header", d_cookie: "Erfasstes Cookie", d_added: "Hinzugefügt",
      val_none: "keine", val_yes: "ja", val_no: "nein", val_unknown: "(unbekannt)",

      detected: "Erkannt",
      pin: "Anheften", unpin: "Lösen", hide: "Ausblenden",
      pin_title: "Immer im Vordergrund umschalten", hide_title: "Dieses Fenster ausblenden",
      filter_ph: "Nach Name oder Seite filtern…",
      clear_items: "Einträge löschen",
      monitoring: "Überwachung", monitoring_title: "Browser-Überwachung stoppen / fortsetzen",
      nothing_detected: "Noch nichts erkannt.",
      no_matches: "Keine Treffer.",
      choose_quality_c: "Qualität wählen:",
      site_c: "Seite: ", link_c: "Vollständiger Link: ",
      added_to_luedd: "Zu Lüdd hinzugefügt!",
      link_expired: "Fehlgeschlagen – Link ist evtl. abgelaufen",
      cant_reach: "Lüdd nicht erreichbar",
    },
  };

  let current = "en";

  function t(key, vars) {
    let s = (STRINGS[current] && STRINGS[current][key]) || STRINGS.en[key] || key;
    if (vars) for (const k in vars) s = s.replace("{" + k + "}", vars[k]);
    return s;
  }

  function applyLang(lang) {
    if (!STRINGS[lang]) lang = "en";
    current = lang;
    document.documentElement.lang = lang;
    document.querySelectorAll("[data-i18n]").forEach(el => {
      el.textContent = t(el.getAttribute("data-i18n"));
    });
    document.querySelectorAll("[data-i18n-placeholder]").forEach(el => {
      el.setAttribute("placeholder", t(el.getAttribute("data-i18n-placeholder")));
    });
    document.querySelectorAll("[data-i18n-title]").forEach(el => {
      el.setAttribute("title", t(el.getAttribute("data-i18n-title")));
    });
  }

  window.I18N = {
    t,
    applyLang,
    get lang() { return current; },
    languages: Object.keys(STRINGS),
  };
})();
