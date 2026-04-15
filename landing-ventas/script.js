const revealItems = document.querySelectorAll(".reveal");
const counterItems = document.querySelectorAll(".metric-number");
const copyButtons = document.querySelectorAll(".copy-btn");
const yearNode = document.getElementById("current-year");
const leadForm = document.getElementById("lead-form");
const autoDataStatus = document.getElementById("auto-data-status");
const autoStatsGrid = document.getElementById("auto-stats-grid");
const latestPicksGrid = document.getElementById("latest-picks-grid");

if (yearNode) {
  yearNode.textContent = new Date().getFullYear();
}

const revealObserver = new IntersectionObserver(
  (entries, observer) => {
    entries.forEach((entry) => {
      if (!entry.isIntersecting) {
        return;
      }

      entry.target.classList.add("is-visible");
      observer.unobserve(entry.target);
    });
  },
  {
    threshold: 0.16,
  }
);

revealItems.forEach((item) => revealObserver.observe(item));

const animateCounter = (node) => {
  const target = Number(node.dataset.target || 0);
  const decimals = Number(node.dataset.decimals || 0);
  const prefix = node.dataset.prefix || "";
  const suffix = node.dataset.suffix || "";
  const duration = 900;
  const start = performance.now();

  const frame = (time) => {
    const progress = Math.min((time - start) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    const rawValue = target * eased;
    const value = decimals > 0
      ? rawValue.toFixed(decimals)
      : String(Math.round(rawValue));

    node.textContent = `${prefix}${value}${suffix}`;

    if (progress < 1) {
      requestAnimationFrame(frame);
    }
  };

  requestAnimationFrame(frame);
};

const counterObserver = new IntersectionObserver(
  (entries, observer) => {
    entries.forEach((entry) => {
      if (!entry.isIntersecting) {
        return;
      }

      animateCounter(entry.target);
      observer.unobserve(entry.target);
    });
  },
  {
    threshold: 0.4,
  }
);

counterItems.forEach((item) => counterObserver.observe(item));

copyButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    const targetId = button.dataset.copyTarget;
    const targetNode = targetId ? document.getElementById(targetId) : null;
    const text = targetNode?.textContent?.trim();

    if (!text) {
      return;
    }

    try {
      await navigator.clipboard.writeText(text);
      const previous = button.textContent;
      button.textContent = "Copiado";
      window.setTimeout(() => {
        button.textContent = previous;
      }, 1600);
    } catch (error) {
      button.textContent = "No se pudo copiar";
      window.setTimeout(() => {
        button.textContent = "Copiar número";
      }, 1600);
    }
  });
});

if (leadForm) {
  leadForm.addEventListener("submit", (event) => {
    event.preventDefault();

    const formData = new FormData(leadForm);
    const name = String(formData.get("name") || "").trim();
    const plan = String(formData.get("plan") || "").trim();
    const level = String(formData.get("level") || "").trim();
    const message = String(formData.get("message") || "").trim();

    const lines = [
      "Hola, vengo de la landing de Erikenobi Picks Premium.",
      `Nombre: ${name}`,
      `Interes principal: ${plan}`,
      `Momento de decision: ${level}`,
    ];

    if (message) {
      lines.push(`Mensaje: ${message}`);
    }

    const text = encodeURIComponent(lines.join("\n"));
    window.open(`https://t.me/erikenobi?text=${text}`, "_blank", "noopener,noreferrer");
  });
}

const monthFormatter = new Intl.DateTimeFormat("es-ES", {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

const resultLabel = (value) => {
  const normalized = String(value || "").toUpperCase();
  if (normalized === "HIT") {
    return { label: "HIT", className: "hit" };
  }
  if (normalized === "MISS") {
    return { label: "MISS", className: "miss" };
  }
  if (normalized === "VOID") {
    return { label: "VOID", className: "void" };
  }
  return { label: "Pend.", className: "pending" };
};

const formatDateTime = (value) => {
  if (!value) {
    return "Fecha no disponible";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Fecha no disponible";
  }

  return monthFormatter.format(date);
};

const renderAutoStats = (data) => {
  if (!autoStatsGrid) {
    return;
  }

  const typeStats = data?.stats?.types || {};
  const preStats = data?.stats?.pre_match_manual;

  const cards = [];

  if (typeStats.gol) {
    cards.push(`
      <article class="auto-card">
        <h3>Goles</h3>
        <span class="auto-highlight">${typeStats.gol.strike}% strike</span>
        <div class="auto-meta">
          <span>${typeStats.gol.total} picks resueltos</span>
          <span>${typeStats.gol.hits} HIT · ${typeStats.gol.misses} MISS · ${typeStats.gol.voids} VOID</span>
        </div>
      </article>
    `);
  }

  if (typeStats.corner) {
    cards.push(`
      <article class="auto-card">
        <h3>Corners</h3>
        <span class="auto-highlight">${typeStats.corner.strike}% strike</span>
        <div class="auto-meta">
          <span>${typeStats.corner.total} picks resueltos</span>
          <span>${typeStats.corner.hits} HIT · ${typeStats.corner.misses} MISS · ${typeStats.corner.voids} VOID</span>
        </div>
      </article>
    `);
  }

  if (preStats) {
    cards.push(`
      <article class="auto-card">
        <h3>Prepartido manual</h3>
        <span class="auto-highlight">${preStats.strike}% strike · ROI ${preStats.roi >= 0 ? "+" : ""}${preStats.roi}%</span>
        <div class="auto-meta">
          <span>${preStats.total} picks resueltos de ${preStats.name}</span>
          <span>${preStats.hits} HIT · ${preStats.misses} MISS · ${preStats.voids} VOID</span>
          <span>Profit ${preStats.profit_units >= 0 ? "+" : ""}${preStats.profit_units}u</span>
        </div>
      </article>
    `);
  }

  autoStatsGrid.innerHTML = cards.join("");
};

const renderPickGroup = (title, picks) => {
  if (!Array.isArray(picks) || picks.length === 0) {
    return `
      <article class="pick-group">
        <h3>${title}</h3>
        <p class="pick-empty">Todavía no hay datos disponibles para este bloque.</p>
      </article>
    `;
  }

  const items = picks
    .map((pick) => {
      const result = resultLabel(pick.result);
      return `
        <article class="pick-card">
          <div class="pick-head">
            <strong>${pick.match || "Partido no disponible"}</strong>
            <span class="result-pill ${result.className}">${result.label}</span>
          </div>
          <p>${pick.market || pick.code || "Pick"}</p>
          <small>${pick.league || "Liga no disponible"} · ${formatDateTime(pick.timestamp)}</small>
        </article>
      `;
    })
    .join("");

  return `
    <article class="pick-group">
      <h3>${title}</h3>
      <div class="pick-list">${items}</div>
    </article>
  `;
};

const renderLatestPicks = (data) => {
  if (!latestPicksGrid) {
    return;
  }

  latestPicksGrid.innerHTML = [
    renderPickGroup("Últimos picks de goles", data?.latest_picks?.goals || []),
    renderPickGroup("Últimos picks de corners", data?.latest_picks?.corners || []),
    renderPickGroup("Últimos picks prepartido", data?.latest_picks?.pre_match || []),
  ].join("");
};

const loadLandingData = async () => {
  if (!autoDataStatus) {
    return;
  }

  try {
    const response = await fetch("./data/landing-data.json", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    renderAutoStats(data);
    renderLatestPicks(data);

    const generatedAt = data?.generated_at ? formatDateTime(data.generated_at) : "fecha desconocida";
    autoDataStatus.textContent = `Datos automáticos cargados. Última generación: ${generatedAt}.`;
  } catch (error) {
    autoDataStatus.textContent =
      "Todavía no hay JSON automático generado. La landing funciona igual, pero este bloque se llenará cuando exportes los datos del bot.";
  }
};

loadLandingData();
