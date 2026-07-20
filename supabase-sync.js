// ============================================================
// Finanzas Tracto (móvil) · Sincronización con Supabase
// ------------------------------------------------------------
// Usa la MISMA base de datos y las MISMAS claves que la app de
// escritorio (FinanceDesk), para que las tareas ("órdenes") que
// se crean en la PC lleguen al celular, y lo que se hace desde
// el celular (depósitos, cheques escaneados) vuelva a la PC.
//
// Mapeo de claves (celular -> tabla compartida fd_store):
//   tareas  -> "fd-tar"
//   ops     -> "fd-ops"
//   agenda  -> "fd-agenda"
// ============================================================

(function () {
  const SUPABASE_URL = "https://vlcootmevguzdoooshan.supabase.co";
  const SUPABASE_ANON_KEY =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZsY29vdG1ldmd1emRvb29zaGFuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODM3ODkwNDIsImV4cCI6MjA5OTM2NTA0Mn0.8UfYG9NGi7MLyqm44NeZMAry5gY4SGjxrGyuD88llic";

  if (typeof window.supabase === "undefined") {
    console.error(
      "[FinTracto] No se encontró la librería supabase-js. Revisá que el <script> del CDN esté antes de supabase-sync.js"
    );
    return;
  }

  const client = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    realtime: { params: { eventsPerSecond: 10 } },
  });

  let dataChannel = null;
  let onRemoteChangeCb = null;

  // Último valor visto/enviado por clave (evita reenvíos innecesarios y
  // corta el eco de Realtime de nuestros propios cambios, para no entrar
  // en un loop infinito de escritura -> notificación -> re-render -> escritura).
  const lastSeen = {};

  async function loadAll() {
    const { data, error } = await client
      .from("fd_store")
      .select("key,value")
      .in("key", ["fd-tar", "fd-ops", "fd-agenda"]);
    if (error) {
      console.error("[FinTracto] Error cargando datos de Supabase:", error.message);
      return null;
    }
    const map = {};
    (data || []).forEach((row) => {
      map[row.key] = row.value;
      lastSeen[row.key] = JSON.stringify(row.value);
    });
    return map;
  }

  async function saveKey(key, value) {
    const str = JSON.stringify(value);
    if (lastSeen[key] === str) return;
    lastSeen[key] = str;
    const { error } = await client
      .from("fd_store")
      .upsert({ key, value, updated_at: new Date().toISOString() }, { onConflict: "key" });
    if (error) console.error("[FinTracto] Error guardando '" + key + "' en Supabase:", error.message);
  }

  let pendingState = null;
  let pushTimer = null;
  function pushAll(state) {
    pendingState = state;
    if (pushTimer) return;
    pushTimer = setTimeout(() => {
      pushTimer = null;
      const s = pendingState;
      pendingState = null;
      saveKey("fd-tar", s.tareas);
      saveKey("fd-ops", s.ops);
      saveKey("fd-agenda", s.agenda);
    }, 400);
  }

  function subscribeDataChanges(cb) {
    onRemoteChangeCb = cb;
    if (dataChannel) return;
    dataChannel = client
      .channel("fd_store_changes_mobile")
      .on(
        "postgres_changes",
        { event: "*", schema: "public", table: "fd_store" },
        (payload) => {
          const row = payload.new && Object.keys(payload.new).length ? payload.new : payload.old;
          if (!row) return;
          if (!["fd-tar", "fd-ops", "fd-agenda"].includes(row.key)) return;
          const str = JSON.stringify(row.value);
          if (lastSeen[row.key] === str) return; // eco de un cambio propio, se ignora
          lastSeen[row.key] = str;
          if (onRemoteChangeCb) onRemoteChangeCb(row.key, row.value);
        }
      )
      .subscribe();
  }

  // ---------- Web Push: notificaciones REALES del sistema operativo ----------
  // A diferencia de un aviso dentro de la app, esto lo entrega el navegador/OS
  // aunque la app esté cerrada (siempre que el celular tenga internet).
  const VAPID_PUBLIC_KEY =
    "BPxmgIsGk77Li4nnIeJwT-QhVyISDQbCbAKO0wDDdi4HDNY9ihU9DWisN5mzfO7v2aYuO5nyfnYd9wGt80KDiRM";

  function urlBase64ToUint8Array(base64String) {
    const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
    const raw = atob(base64);
    const arr = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; ++i) arr[i] = raw.charCodeAt(i);
    return arr;
  }

  async function subscribePush() {
    if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
      console.warn("[FinTracto] Este navegador no soporta notificaciones push.");
      return false;
    }
    try {
      if (Notification.permission === "default") await Notification.requestPermission();
      if (Notification.permission !== "granted") {
        console.warn("[FinTracto] Permiso de notificaciones no concedido.");
        return false;
      }
      const reg = await navigator.serviceWorker.ready;
      let sub = await reg.pushManager.getSubscription();
      // Si ya había una suscripción pero con una clave VAPID distinta a la
      // actual (por ejemplo, si se rotaron las claves), se descarta y se
      // vuelve a crear para que quede al día automáticamente.
      if (sub) {
        const currentKey = sub.options && sub.options.applicationServerKey
          ? new Uint8Array(sub.options.applicationServerKey)
          : null;
        const expectedKey = urlBase64ToUint8Array(VAPID_PUBLIC_KEY);
        const distinta = !currentKey || currentKey.length !== expectedKey.length ||
          currentKey.some((b, i) => b !== expectedKey[i]);
        if (distinta) {
          await sub.unsubscribe().catch(() => {});
          sub = null;
        }
      }
      if (!sub) {
        sub = await reg.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlBase64ToUint8Array(VAPID_PUBLIC_KEY),
        });
      }
      const json = sub.toJSON();
      const { error } = await client
        .from("push_subscriptions")
        .upsert(
          { endpoint: json.endpoint, p256dh: json.keys.p256dh, auth: json.keys.auth },
          { onConflict: "endpoint" }
        );
      if (error) {
        console.error("[FinTracto] No se pudo guardar la suscripción push en Supabase:", error.message);
        return false;
      }
      return true;
    } catch (e) {
      console.error("[FinTracto] No se pudo suscribir a push:", e);
      return false;
    }
  }

  // Dispara una notificación real a TODOS los dispositivos suscriptos
  // (se llama, por ejemplo, apenas se crea una tarea nueva).
  async function sendPushTrigger(title, body) {
    try {
      await fetch(SUPABASE_URL + "/functions/v1/send-push", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: "Bearer " + SUPABASE_ANON_KEY },
        body: JSON.stringify({ title, body }),
      });
    } catch (e) {
      console.error("[FinTracto] No se pudo disparar la notificación push:", e);
    }
  }

  // ---------- Acuses (comprobantes/fotos) en su PROPIA tabla ----------
  // Antes la imagen viajaba adentro de "tareas"/"agenda" (el blob gigante
  // que también se guarda en localStorage), y una sola foto de cámara podía
  // superar el límite de localStorage (~5-10MB) y hacer fallar TODO el
  // guardado sin aviso. Ahora la imagen va sola, directo a Supabase.
  async function saveAcuse(banco, txt, monto, mon, fecha, imagenBase64) {
    const { error } = await client
      .from("acuses")
      .insert({ banco, txt, monto: monto || null, mon: mon || null, fecha, imagen: imagenBase64 });
    if (error) {
      console.error("[FinTracto] Error guardando el acuse:", error.message);
      return false;
    }
    return true;
  }

  async function loadAcuses() {
    const { data, error } = await client
      .from("acuses")
      .select("id,banco,txt,monto,mon,fecha,imagen")
      .order("fecha", { ascending: false });
    if (error) {
      console.error("[FinTracto] Error cargando acuses:", error.message);
      return [];
    }
    return data || [];
  }

  // ---------- Broadcast: avisa a la PC en vivo de cada cambio de fase ----------
  // (iniciar / llegué al banco / terminar), en el mismo canal que ya escucha
  // la app de escritorio (fd-events), para que le dispare su notificación.
  let eventsChannelReady = null;
  function ensureEventsChannel() {
    if (!eventsChannelReady) {
      eventsChannelReady = new Promise((resolve) => {
        const ch = client.channel("fd-events", { config: { broadcast: { self: false } } });
        ch.subscribe((status) => {
          if (status === "SUBSCRIBED") resolve(ch);
        });
      });
    }
    return eventsChannelReady;
  }
  function broadcastEvent(obj) {
    ensureEventsChannel().then((ch) => ch.send({ type: "broadcast", event: "fd", payload: obj }));
  }

  window.FDSupabase = {
    client,
    loadAll,
    saveKey,
    pushAll,
    subscribeDataChanges,
    subscribePush,
    sendPushTrigger,
    saveAcuse,
    loadAcuses,
    broadcastEvent,
  };
})();
