const { createApp } = Vue;

const EVENT_LABELS = {
  extract_fail: "抽取失败",
  auto_retry: "自动重试失败",
  retry_ok: "重试成功",
  manual_retry: "人工重试",
  auto_stop: "满次停自动",
  to_dead: "转入死信",
  to_manual: "转人工触发",
  requeue: "死信回炉",
  contention: "重试争抢",
  exit_skew: "出路写歪",
  blocked_full: "全量拦截",
};

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      overview: {
        totals: { queue: 0, retrying: 0, manual: 0, dead: 0, success: 0 },
        pipelines: [],
        settings: {},
      },
      busy: false,
      msg: "",
      msgErr: false,
      showEvents: false,
      exitChoice: {},
      requeueReason: {},
      settingsForm: { max_auto_retries: 3, retry_delay_seconds: 10 },
      timer: null,
    };
  },
  computed: {
    statTiles() {
      return [
        { key: "retrying", label: "重试中" },
        { key: "dead", label: "死信数" },
        { key: "manual", label: "人工待点" },
        { key: "queue", label: "队列待重试" },
      ];
    },
    queuedRows() {
      if (!this.current) return [];
      return this.current.rows.filter(
        (r) => r.state === "队列中" || r.state === "重试中" || r.state === "人工待点"
      );
    },
    deadRows() {
      if (!this.current) return [];
      return this.current.rows.filter((r) => r.state === "死信");
    },
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await this.refresh();
    // 服务端调度在后台跑，前端周期刷新置顶队列/死信/总览
    this.timer = setInterval(() => this.refresh(true), 3000);
  },
  beforeUnmount() {
    if (this.timer) clearInterval(this.timer);
  },
  methods: {
    eventLabel(kind) {
      return EVENT_LABELS[kind] || kind;
    },
    tagClass(status) {
      if (status === "已落库" || status === "已成功") return "ok";
      if (status === "已抽取待加载") return "warn";
      if (status === "抽取失败") return "danger";
      return "";
    },
    rowStateClass(state) {
      if (state === "重试中") return "retrying";
      if (state === "人工待点") return "manual";
      if (state === "队列中") return "queue";
      return "";
    },
    setMsg(text, isErr = false) {
      this.msg = text;
      this.msgErr = isErr;
    },
    async refresh(silent = false) {
      const [pipes, ov] = await Promise.all([
        fetch("/api/pipelines"),
        fetch("/api/retry/overview"),
      ]);
      if (pipes.ok) this.items = (await pipes.json()).items || [];
      if (ov.ok) this.overview = await ov.json();
      if (this.current) {
        const keep = this.current.id;
        const res = await fetch(`/api/pipelines/${keep}`);
        if (res.ok) {
          this.current = await res.json();
          this.settingsForm = { ...this.current.settings };
        }
      } else if (!silent) {
        this.setMsg("");
      }
    },
    async open(id) {
      const res = await fetch(`/api/pipelines/${id}`);
      if (!res.ok) return;
      this.current = await res.json();
      this.settingsForm = { ...this.current.settings };
      this.exitChoice = {};
      this.requeueReason = {};
      this.setMsg("");
    },
    async extract() {
      if (!this.current) return;
      this.busy = true;
      this.setMsg("全量抽取进行中…");
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, { method: "POST" });
        const data = await res.json();
        if (!res.ok) {
          this.setMsg(data.detail || "抽取失败", true);
        } else {
          this.current = data;
          const fails = data.counts.dead + data.counts.queue + data.counts.manual + data.counts.retrying;
          this.setMsg(fails ? `抽取完成，${fails} 行失败进入重试队列` : "抽取完成，可继续落库");
        }
        await this.refresh(true);
      } finally {
        this.busy = false;
      }
    },
    async load() {
      if (!this.current) return;
      this.busy = true;
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, { method: "POST" });
        const data = await res.json();
        if (!res.ok) {
          this.setMsg(data.detail || "落库失败", true);
        } else {
          this.current = data;
          this.setMsg(data.status === "已落库" ? "落库完成" : "失败行未全部救回，暂不能落库", data.status !== "已落库");
        }
        await this.refresh(true);
      } finally {
        this.busy = false;
      }
    },
    async retryRow(r) {
      const exit = this.exitChoice[r.id] || null;
      const res = await fetch(`/api/retry/rows/${r.id}/retry`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ exit_path: exit }),
      });
      const data = await res.json();
      if (!res.ok) {
        this.setMsg(data.detail || "重试未生效", true);
      } else {
        this.setMsg(
          data.state === "已成功"
            ? `行 ${r.row_key} 重试成功`
            : `行 ${r.row_key} 重试后仍为「${data.state}」，见事件记录`
        );
      }
      await this.refresh(true);
    },
    async requeueRow(r) {
      const reason = (this.requeueReason[r.id] || "").trim();
      if (!reason) {
        this.setMsg("死信行须填原因才能回炉", true);
        return;
      }
      const res = await fetch(`/api/retry/rows/${r.id}/requeue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason }),
      });
      const data = await res.json();
      if (!res.ok) {
        this.setMsg(data.detail || "回炉失败", true);
      } else {
        this.requeueReason[r.id] = "";
        this.setMsg(`行 ${r.row_key} 已回炉，进入队列且仅人工触发`);
      }
      await this.refresh(true);
    },
    async saveSettings() {
      const res = await fetch("/api/retry/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(this.settingsForm),
      });
      const data = await res.json();
      if (!res.ok) {
        this.setMsg(data.detail || "设置保存失败", true);
      } else {
        this.settingsForm = { ...data };
        this.setMsg("重试设置已保存");
      }
      await this.refresh(true);
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
