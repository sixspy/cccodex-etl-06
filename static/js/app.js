const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      busy: false,
      msg: "",
    };
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await this.refresh();
  },
  methods: {
    tagClass(status) {
      if (status === "已落库") return "ok";
      if (status === "已抽取待加载") return "warn";
      return "";
    },
    async refresh() {
      const res = await fetch("/api/pipelines");
      if (!res.ok) return;
      this.items = (await res.json()).items || [];
    },
    async open(id) {
      const res = await fetch(`/api/pipelines/${id}`);
      if (!res.ok) return;
      this.current = await res.json();
      this.msg = "";
    },
    async extract() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, {
          method: "POST",
        });
        this.current = await res.json();
        this.msg = "抽取完成，可继续落库";
        await this.refresh();
      } finally {
        this.busy = false;
      }
    },
    async load() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, {
          method: "POST",
        });
        this.current = await res.json();
        this.msg =
          this.current.status === "已落库"
            ? "落库完成"
            : "当前状态不能落库，请先抽取";
        await this.refresh();
      } finally {
        this.busy = false;
      }
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
