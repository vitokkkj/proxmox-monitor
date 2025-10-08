// FileName: /script.js
/* ===== CONFIG ===== */
const RECENT_DOTS = 12; // <<< quantas bolinhas mostrar
const STALE_SECS = 24 * 3600; // atraso: 24h sem backup
const NAIVE_TZ_OFFSET_HOURS = 0;
const MIN_UPDATE_INTERVAL = 10000;
const MODAL_ITEMS_PER_PAGE = 10; // quantidade de Itens na páginação do modal
let lastUpdateTime = 0;
// clientes que, quando vier "DD/MM/YYYY" ambíguo, devem ser interpretados como D/M
window._forceDayMonthFor = [
  /Proxmox Matheus/i, // ajuste aqui o(s) nome(s) exato(s) que aparecem no card
];
// --- Proteções contra sobrecarga / duplicação de timers ---
let LOAD_TIMER = null;
let inFlight = false;
let loadSummariesController = null;
let currentModalPage = 1;

/* ===== helpers ===== */
function fmtBytes(b) {
  if (b == null || isNaN(b)) return "—";
  const u = ["B", "KB", "MB", "GB", "TB", "PB"];
  let i = 0,
    x = Number(b);
  while (x >= 1024 && i < u.length - 1) {
    x /= 1024;
    i++;
  }
  return `${x.toFixed(i ? 2 : 0)} ${u[i]}`;
}
function fmtSpeed(x) {
  return !x || isNaN(x) || x <= 0 ? "N/A MB/s" : `${Number(x).toFixed(2)} MB/s`;
}

function safe(arr) {
  return Array.isArray(arr) ? arr : [];
}

function escAttr(s) {
  return String(s ?? "").replace(/"/g, "&quot;");
}

function updateTopBadge() {
  const el = document.getElementById("last-updated");
  if (!el) return;
  const now = new Date();
  el.textContent = `Última atualização: ${formatDateBR(now)}`;
}

// Converte datas para objetos Date, aplicando offset do fuso
function parseAsUTCOrISO(raw, offsetHours = NAIVE_TZ_OFFSET_HOURS) {
  if (!raw) return null;
  // Função auxiliar para aplicar offset
  const withOffset = (d) => {
    if (!(d instanceof Date) || isNaN(d)) return null;
    const result = new Date(d);
    return result;
  };
  const s = String(raw).trim();
  // Se for timestamp Unix em segundos
  if (typeof raw === "number" || /^\d+$/.test(s)) {
    const timestamp = Number(s);
    return withOffset(
      new Date(timestamp < 1e12 ? timestamp * 1000 : timestamp)
    );
  }
  // Tenta primeiro formato ISO (YYYY-MM-DDTHH:MM:SSZ ou YYYY-MM-DD HH:MM:SS)
  if (/\dT\d.*(Z|[+-]\d{2}:\d{2})$/.test(s)) {
    // Ex: 2023-03-15T10:00:00Z
    const t = Date.parse(s);
    if (!isNaN(t)) return withOffset(new Date(t));
  }
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/); // Ex: 2023-03-15 10:00:00
  if (m) {
    const [, Y, Mo, D, H, Mi, S] = m.map(Number);
    // Para este formato, o construtor Date(Y, M-1, D, H, M, S) é seguro e interpreta corretamente.
    return withOffset(new Date(Y, Mo - 1, D, H, Mi, S));
  }
  // Tenta formato brasileiro (DD/MM/YYYY HH:MM:SS)
  m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})[,\s]\s*(\d{2}):(\d{2}):(\d{2})$/);
  if (m) {
    let [, D, Mo, Y, H, Mi, S] = m.map(Number); // Captura diretamente como Dia, Mês, Ano
    // O construtor Date(year, monthIndex, day, hours, minutes, seconds) espera monthIndex (0-11)
    return withOffset(new Date(Y, Mo - 1, D, H, Mi, S));
  }
  // Tenta parsear como ISO genérico (fallback)
  const parsed = new Date(s);
  return isNaN(parsed.getTime()) ? null : withOffset(parsed);
}

function formatDateBR(date) {
  if (!date || !(date instanceof Date) || isNaN(date.getTime())) return "—";
  const dia = date.getDate().toString().padStart(2, "0");
  const mes = (date.getMonth() + 1).toString().padStart(2, "0");
  const ano = date.getFullYear();
  const hora = date.getHours().toString().padStart(2, "0");
  const minuto = date.getMinutes().toString().padStart(2, "0");
  const segundo = date.getSeconds().toString().padStart(2, "0");
  return `${dia}/${mes}/${ano} ${hora}:${minuto}:${segundo}`;
}

function tsToStr(epoch) {
  if (!epoch) return "—";
  const date = parseAsUTCOrISO(epoch);
  return date ? formatDateBR(date) : "—";
}

function fmtReceived(at) {
  if (!at) return "";
  // Reutiliza parseAsUTCOrISO para consistência
  const date = parseAsUTCOrISO(at);
  return date ? formatDateBR(date) : String(at);
}

function normalizeNaiveTimestamps(rootEl) {
  if (!rootEl) return;
  const walker = document.createTreeWalker(rootEl, NodeFilter.SHOW_TEXT);
  const nodes = [];
  // Ajusta a regex para capturar timestamps com ou sem 'T' e com ou sem 'atualizado:'
  const re =
    /\(?(?:atualizado:\s*)?(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}|\d{2}\/\d{2}\/\d{4}[,\s]\s*\d{2}:\d{2}:\d{2})\)?/g;
  while (walker.nextNode()) nodes.push(walker.currentNode);
  nodes.forEach((node) => {
    const txt = node.nodeValue;
    if (!txt || !re.test(txt)) return;
    node.nodeValue = txt.replace(re, (full, ts) => {
      const local = fmtReceived(ts);
      const hasParens = /^\(.*\)$/.test(full.trim());
      const label = /atualizado:/i.test(full) ? "atualizado: " : "";
      const rendered = label + (local || ts);
      return hasParens ? `(${rendered})` : rendered;
    });
  });
}

/* ===== Modal helpers ===== */
const companyModal = {
  overlay: document.getElementById("company-modal"),
  title: document.getElementById("company-modal-title"),
  body: document.getElementById("company-modal-body"),
  close: document.getElementById("company-modal-close"),
};

// Função auxiliar para formatar um backup para exibição no modal
function formatBackupForModal(backup) {
  const statusIcon =
    backup.status === "SUCCESS"
      ? "✅"
      : ["ERROR", "FAIL"].includes(backup.status)
      ? "❌"
      : "⚠️";
  const writtenSize = backup.written_size_bytes
    ? `${(backup.written_size_bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
    : "N/A GB";

  return `
    <tr class="hover:bg-[var(--row-hover-bg)] border-b border-[var(--border-color)] last:border-b-0">
      <td class="py-3 px-4 font-bold flex items-center gap-2">
        <span>${statusIcon}</span>
        <span class="status-${backup.status}">${backup.status}</span>
      </td>
      <td class="py-3 px-4 whitespace-nowrap">${backup.proxmox_host}</td>
      <td class="py-3 px-4 whitespace-nowrap">
        ${backup.vm_name || "ID: " + backup.vmid} (${backup.vmid})
      </td>
      <td class="py-3 px-4 whitespace-nowrap font-semibold">
        ${backup.storage_target || "N/D"}
      </td>
      <td class="py-3 px-4 whitespace-nowrap cell-dt" title="${tsToStr(
        backup.start_time
      )}">${formatDateBRShort(parseAsUTCOrISO(backup.start_time))}</td>
      <td class="py-3 px-4 whitespace-nowrap cell-dt" title="${tsToStr(
        backup.end_time
      )}">${formatDateBRShort(parseAsUTCOrISO(backup.end_time))}</td>
      <td class="py-3 px-4 whitespace-nowrap">${formatDuration(
        backup.duration_seconds
      )}</td>
      <td class="py-3 px-4 whitespace-nowrap cell-num cell-bytes">${writtenSize}</td>
    </tr>
  `;
}

// Função auxiliar para formatar a duração
function formatDuration(seconds) {
  if (seconds === null || isNaN(seconds)) return "0:00:00";
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;
  return `${hours}:${String(minutes).padStart(2, "0")}:${String(
    remainingSeconds
  ).padStart(2, "0")}`;
}

// Função auxiliar para formatar data para o modal (DD/MM HH:MM)
function formatDateBRShort(date) {
  if (!date || !(date instanceof Date) || isNaN(date.getTime())) return "—";
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const HH = String(date.getHours()).padStart(2, "0");
  const MM = String(date.getMinutes()).padStart(2, "0");
  return `${dd}/${mm} ${HH}:${MM}`;
}

async function openCompanyModal(companyName, page = 1) {
  window._currentCompany = companyName || "";
  currentModalPage = page; // Atualiza a página atual
  companyModal.title.textContent = companyName;
  companyModal.body.innerHTML =
    '<div class="text-center p-4">Carregando dados...</div>';
  companyModal.overlay.style.display = "flex";
  try {
    // 1. Buscar dados detalhados do cliente com paginação
    const res = await fetch(
      `/api/company/${encodeURIComponent(
        companyName
      )}/recent?page=${page}&per_page=${MODAL_ITEMS_PER_PAGE}`
    );
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
    const data = await res.json();
    const backups = data.backups;
    const pagination = data.pagination;
    // 2. Buscar dados de saúde (já temos no cache global __companiesCache)
    const companyDataFromCache = (window.__companiesCache || []).find(
      (x) => (x.company_name || "") === companyName
    );
    const healthByHost = companyDataFromCache
      ? companyDataFromCache.health
      : {};
    let modalBodyHtml = "";

    // --- SAÚDE DO ARMAZENAMENTO ---
    if (Object.keys(healthByHost).length > 0) {
      let healthHtmlContent = "";
      Object.entries(healthByHost).forEach(([hostname, h]) => {
        const pools = h.pools || []; // Já normalizado no backend
        const pillsHtml = pools.length
          ? pools
              .map((p) => {
                const pname = p.name || "?";
                const pstatus = String(p.status || "UNKNOWN").toUpperCase();
                let klass = "health-badge health-unknown";
                if (pstatus === "ONLINE") klass = "health-badge health-online";
                else if (pstatus === "DEGRADED")
                  klass = "health-badge health-degraded";
                else if (["FAULTED", "OFFLINE", "UNAVAIL"].includes(pstatus))
                  klass = "health-badge health-faulted";
                return `<span class="${klass}">${pname}: ${pstatus}</span>`;
              })
              .join("")
          : '<span class="text-[#aaa]">sem dados de pools</span>';

        healthHtmlContent += `
          <div class="mt-2">
            <div class="text-[#ccc] font-medium">
              Host: ${hostname}
              ${
                h.received_at
                  ? `<span class="text-xs text-[#9aa] ml-2">(atualizado: ${fmtReceived(
                      h.received_at
                    )})</span>`
                  : ""
              }
            </div>
            <div class="health-grid mt-2">
              ${pillsHtml}
            </div>
          </div>
        `;
      });

      modalBodyHtml += `
        <div class="px-4 pt-4 pb-1 border-b border-[var(--border-color)]">
          <strong class="text-[1.05rem]">Saúde do Armazenamento</strong>
          ${healthHtmlContent}
        </div>
      `;
    }

    // --- TABELA DE BACKUPS ---
    if (backups.length > 0) {
      const backupRowsHtml = backups.map(formatBackupForModal).join("");
      modalBodyHtml += `
        <div class="overflow-x-auto">
          <table class="w-full border-collapse">
            <thead class="bg-[#333]">
              <tr>
                <th class="py-3 px-4 text-left whitespace-nowrap">Status</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Host</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">VM/CT</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Destino</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Início</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Fim</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Duração</th>
                <th class="py-3 px-4 text-left whitespace-nowrap">Escrito</th>
              </tr>
            </thead>
            <tbody>
              ${backupRowsHtml}
            </tbody>
          </table>
        </div>
        <div class="pagination-controls text-center mt-4 flex justify-center items-center gap-4">
          <button id="prev-page" class="px-4 py-2 bg-gray-700 text-white rounded-md disabled:opacity-50" ${
            pagination.current_page === 1 ? "disabled" : ""
          }>Anterior</button>
          <span>Página ${pagination.current_page} de ${
        pagination.total_pages
      }</span>
          <button id="next-page" class="px-4 py-2 bg-gray-700 text-white rounded-md disabled:opacity-50" ${
            pagination.current_page === pagination.total_pages ? "disabled" : ""
          }>Próximo</button>
        </div>
      `;
    } else if (Object.keys(healthByHost).length === 0) {
      modalBodyHtml +=
        '<div class="p-6 text-center text-[#888]">Nenhum dado para este cliente.</div>';
    }

    companyModal.body.innerHTML = modalBodyHtml;
    normalizeNaiveTimestamps(companyModal.body);
    // Adicionar listeners para os botões de paginação
    const prevButton = document.getElementById("prev-page");
    const nextButton = document.getElementById("next-page");
    if (prevButton) {
      prevButton.onclick = () =>
        openCompanyModal(companyName, currentModalPage - 1);
    }
    if (nextButton) {
      nextButton.onclick = () =>
        openCompanyModal(companyName, currentModalPage + 1);
    }
    // --- REPLICAÇÃO (do cache, pois a API /recent não retorna) ---
    const rep = companyDataFromCache && companyDataFromCache.replication;
    if (rep && Array.isArray(rep.jobs) && rep.jobs.length) {
      const rows = rep.jobs
        .map(
          (j) => `
      <tr>
        <td>${j.vmid || ""}</td>
        <td>${(j.vm_name || "").replace(/</g, "&lt;")}</td>
        <td>${j.source_node || ""}</td>
        <td>${j.target_node || ""}</td>
        <td>${
          j.last_sync_str ||
          (j.last_sync
            ? new Date(j.last_sync * 1000).toLocaleString("pt-BR")
            : "—")
        }</td>
        <td>${j.duration_sec ?? ""}</td>
        <td>${j.fail_count ?? 0}</td>
        <td class="${
          (j.status || "").toUpperCase() === "SUCCESS" ? "t-ok" : "t-err"
        }">${j.status || ""}</td>
      </tr>
    `
        )
        .join("");

      const box = document.createElement("div");
      box.className = "accordion";
      box.innerHTML = `
        <button class="accordion-header">
          <span>Replicação (últimos jobs)</span>
          <span class="accordion-icon">▶</span>
        </button>
        <div class="accordion-body">
          <div class="table-wrapper">
            <table class="compact-table">
              <thead>
                <tr>
                  <th>VMID</th><th>Nome</th><th>Source</th><th>Target</th>
                  <th>Últ. Sync</th><th>Duração (s)</th><th>Fails</th><th>Status</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>`;
      companyModal.body.appendChild(box);

      const header = box.querySelector(".accordion-header");
      const body = box.querySelector(".accordion-body");
      const icon = box.querySelector(".accordion-icon");
      header.addEventListener("click", () => {
        body.classList.toggle("open");
        header.classList.toggle("active");
        icon.style.transform = body.classList.contains("open")
          ? "rotate(90deg)"
          : "rotate(0deg)";
      });
    }
  } catch (e) {
    console.error("Erro ao carregar dados do modal:", e);
    companyModal.body.innerHTML = `<div class="p-6 text-center text-red-500">Erro ao carregar dados: ${e.message}</div>`;
  }
}

function closeCompanyModal() {
  companyModal.overlay.style.display = "none";
}

/* ===== cards de resumo (saúde no rodapé + chips alinhados + replicação) ===== */
// Função para renderizar a saúde do armazenamento como pills numeradas
function renderHealthInlineHTML(healthByHost) {
  if (!healthByHost || Object.keys(healthByHost).length === 0) return "";

  let allHealthPillsHtml = [];
  let itemCounter = 1; // Contador para numerar os itens

  Object.entries(healthByHost).forEach(([host, h]) => {
    // Processar pools (ZFS, etc.)
    (h.pools || []).forEach((p) => {
      const name = p.name || "?";
      const status = String(p.status || "UNKNOWN").toUpperCase();
      let statusClass = "pill-warn"; // Default para UNKNOWN/WARN
      let tip = `Host: ${host} • Pool: ${name} • Status: ${status}`;

      if (status === "ONLINE") {
        statusClass = "pill-ok"; // Usar pill-ok para sucesso
      } else if (status === "DEGRADED") {
        statusClass = "pill-warn";
      } else if (
        status === "FAULTED" ||
        status === "OFFLINE" ||
        status === "UNAVAIL"
      ) {
        statusClass = "pill-fail"; // Usar pill-fail para falha
      }
      allHealthPillsHtml.push(
        `<span class="pill ${statusClass}" title="${escAttr(
          tip
        )}">DISK ${itemCounter++}: ${status}</span>`
      );
    });

    // Processar discos SMART (se houver)
    // Assumindo que 'h.disks' pode existir e conter 'smart_ok'
    (h.disks || []).forEach((d) => {
      const name = d.name || "?";
      const smartOk = d.smart_ok;
      let statusClass = "pill-warn"; // Default para UNKNOWN/WARN
      let tip = `Host: ${host} • Disco: ${name} • SMART: ${
        smartOk ? "OK" : "FALHA"
      }`;

      if (smartOk === true) {
        statusClass = "pill-ok";
      } else if (smartOk === false) {
        statusClass = "pill-fail";
      }
      allHealthPillsHtml.push(
        `<span class="pill ${statusClass}" title="${escAttr(
          tip
        )}">Disk ${itemCounter++}: ${smartOk ? "OK" : "FALHA"}</span>`
      );
    });
  });

  if (allHealthPillsHtml.length === 0) {
    return '<span class="pill pill-unknown">sem dados de discos</span>'; // Retorna uma pill de "sem dados"
  }

  // Retorna as pills agrupadas em uma div para alinhamento
  return `<div class="health-pills-row">${allHealthPillsHtml.join("")}</div>`;
}

/* ===== cards de resumo (saúde no rodapé + chips alinhados + replicação) ===== */
function renderSummaryCard(c) {
  if (!c || typeof c !== "object") return "";

  const recent = safe(c.recent_backups || c.recent || []).slice(0, RECENT_DOTS);

  const ok = c.stats_24h.ok || 0;
  const fail = c.stats_24h.fail || 0;
  const tot = c.stats_24h.total || 0;

  // === DOTS BACKUP ===
  const realDots = recent.map((r) => {
    const statusClass = r.status === "SUCCESS" ? "success" : "fail";
    const who = r.vm_name || "ID: " + (r.vmid ?? "");
    const tip = `${who} • ${r.status} • ${tsToStr(
      r.end_time
    )} • escrito ${fmtBytes(r.written_size_bytes)}`;
    return `<span class="dot ${statusClass}" data-tip="${escAttr(
      tip
    )}"></span>`;
  });

  const ghostCount = Math.max(0, RECENT_DOTS - realDots.length);
  const ghostDots = Array.from(
    { length: ghostCount },
    () => `<span class="dot warn" data-tip="sem dado"></span>`
  );

  const dotsHtml =
    realDots.join("") + ghostDots.join("") ||
    '<span style="color:#666">sem dados</span>';

  // === REPLICAÇÃO ===
  const repl = c.replication || {};
  const jobs = Array.isArray(repl.jobs) ? repl.jobs : [];
  let replPills = [];

  if (jobs.length > 0) {
    replPills = jobs.map((j) => {
      const statusClass = j.status === "SUCCESS" ? "pill-ok" : "pill-fail";
      const lastStr = j.last_sync
        ? new Date(j.last_sync * 1000).toLocaleString("pt-BR")
        : j.last_sync_str || "—";
      const tip = `VM ${j.vmid} (${j.vm_name || "?"}) • ${
        j.status
      } • ${lastStr}`;
      return `<span class="pill ${statusClass}" title="${escAttr(tip)}">
                VM ${j.vmid}: ${j.status}
              </span>`;
    });
  } else {
    replPills.push(`<span class="pill pill-warn">Sem replicação</span>`);
  }

  const replLast = repl.last_sync
    ? new Date(repl.last_sync * 1000).toLocaleString("pt-BR")
    : repl.last_sync_str || "—";

  const replHtml = `
    <div class="summary-section summary-repl">
      <div class="summary-label">Replicação</div>
      <div class="repl-pills">${replPills.join("")}</div>
      <div class="summary-meta text-xs mt-1">Últ. replicação: ${replLast}</div>
    </div>
  `;

  // === STATUS DO CARD ===
  const now = Math.floor(Date.now() / 1000);
  const last = c.last_update || 0;
  const isStale = !last || now - last > STALE_SECS;

  const lastBackup = recent.length > 0 ? recent[0] : null;
  const lastBackupFailed = lastBackup && lastBackup.status !== "SUCCESS";

  let cardClasses =
    "client-card bg-[var(--card-bg)] rounded-lg shadow-lg overflow-hidden";
  if (lastBackupFailed) cardClasses += " error";
  else if (isStale) cardClasses += " stale";

  const warnIcon = lastBackupFailed ? " • ⚠" : isStale ? " • ⏰" : "";

  // última atualização
  let lastStr = "—";
  if (c.last_update) {
    const timestamp = Number(c.last_update);
    if (!isNaN(timestamp)) {
      const date = new Date(timestamp * 1000);
      lastStr = formatDateBR(date);
    }
  } else if (c.last_update_str) {
    const date = parseAsUTCOrISO(c.last_update_str);
    if (date instanceof Date && !isNaN(date)) {
      lastStr = formatDateBR(date);
    }
  }

  // CHAMA A FUNÇÃO MODIFICADA PARA GERAR PILLS DE SAÚDE
  const healthPillsHtml = renderHealthInlineHTML(c.health);

  // A seção de saúde agora volta a ser a summary-health original
  const healthSectionHtml = `
    <div class="summary-section summary-health">
      <div class="summary-label">Discos</div>
      ${healthPillsHtml}
    </div>
  `;

  return `
    <div class="${cardClasses}" data-company="${escAttr(c.company_name)}">
      <div class="summary-content-wrapper">
        <div class="summary-header">
          <div class="summary-title">${c.company_name || "—"}</div>
          <div class="summary-meta">últ. backup: ${lastStr}${warnIcon}</div>
        </div>

        <div class="summary-section summary-backups">
          <div class="summary-backups-header">
            <span class="summary-label">Backups</span>
          </div>
          <div class="dot-row centered">${dotsHtml}</div>
        </div>

        <div class="summary-section summary-row">
          <span class="summary-chip">24h ✔ ${ok}</span>
          <span class="summary-chip">24h ✖ ${fail}</span>
          <span class="summary-chip">24h total ${tot}</span>
        </div>

        ${replHtml}

        ${healthSectionHtml} <!-- VOLTA A USAR A SEÇÃO summary-health -->

        <!-- Botão fixo no canto inferior direito -->
        <button class="pill pill-cta btn-vermais"
        onclick="openCompanyModal('${escAttr(c.company_name)}')">
        ▸ Ver mais
        </button>
      </div>
    </div>
  `;
}

let globalTooltip = null;
function createGlobalTooltip() {
  globalTooltip = document.createElement('div');
  globalTooltip.id = 'global-tooltip';
  document.body.appendChild(globalTooltip);
}
function showTooltip(event) {
  const target = event.target;
  const tip = target.getAttribute('data-tip');
  if (!tip || !globalTooltip) return;
  globalTooltip.textContent = tip;
  globalTooltip.classList.add('visible');
  // Posicionar o tooltip
  const rect = target.getBoundingClientRect();
  let top = rect.top - globalTooltip.offsetHeight - 10; // 10px acima do dot
  let left = rect.left + (rect.width / 2) - (globalTooltip.offsetWidth / 2);
  // Ajustar se sair da tela à esquerda
  if (left < 5) {
    left = 5;
  }
  // Ajustar se sair da tela à direita
  if (left + globalTooltip.offsetWidth > window.innerWidth - 5) {
    left = window.innerWidth - globalTooltip.offsetWidth - 5;
  }
  // Ajustar se sair da tela para cima
  if (top < 5) {
    top = rect.bottom + 10; // 10px abaixo do dot
  }
  globalTooltip.style.top = `${top}px`;
  globalTooltip.style.left = `${left}px`;
}
function hideTooltip() {
  if (globalTooltip) {
    globalTooltip.classList.remove('visible');
  }
}

async function loadSummaries() {
  // Verifica se já passou tempo suficiente desde a última atualização
  const now = Date.now();
  if (now - lastUpdateTime < MIN_UPDATE_INTERVAL) {
    return;
  }
  lastUpdateTime = now;
  if (inFlight) return;
  inFlight = true;
  try {
    if (loadSummariesController) loadSummariesController.abort();
    loadSummariesController = new AbortController();
    const res = await fetch(`/api/companies?limit=${RECENT_DOTS}`, {
      cache: "no-store",
      signal: loadSummariesController.signal,
    });
    const data = await res.json();
    const arr = Array.isArray(data) ? data : [];
    window.__companiesCache = arr; // Armazena em cache para o modal

    const grid = document.getElementById("summary-grid");
    grid.innerHTML = ""; // Limpa o grid antes de preencher

    if (!arr.length) {
      grid.innerHTML = '<div class="text-center p-4">Nenhum dado disponível</div>';
    } else {
      arr.forEach(companyData => {
        grid.innerHTML += renderSummaryCard(companyData);
      });
      // Adicionar listeners para os dots após renderizar os cards
      document.querySelectorAll('.dot').forEach(dot => {
        dot.addEventListener('mouseover', showTooltip);
        dot.addEventListener('mouseout', hideTooltip);
      });
    }

    normalizeNaiveTimestamps(grid);
    updateTopBadge();
  } catch (e) {
    if (e.name !== "AbortError") {
      console.error("Erro ao carregar resumo:", e);
      const grid = document.getElementById("summary-grid");
      if (grid) {
        grid.innerHTML =
          '<div class="text-center p-4 error">Erro ao carregar dados</div>';
      }
    }
  } finally {
    inFlight = false;
  }
}

document.addEventListener("DOMContentLoaded", function () {
  updateTopBadge();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      if (LOAD_TIMER) {
        clearInterval(LOAD_TIMER);
        LOAD_TIMER = null;
      }
    } else {
      if (!LOAD_TIMER) LOAD_TIMER = setInterval(loadSummaries, 30_000);
      loadSummaries();
    }
  });
  createGlobalTooltip();
  if (companyModal.close)
    companyModal.close.addEventListener("click", closeCompanyModal);
  if (companyModal.overlay)
    companyModal.overlay.addEventListener("click", (e) => {
      if (e.target === companyModal.overlay) closeCompanyModal();
    });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeCompanyModal();
  });
  loadSummaries();
  if (LOAD_TIMER) clearInterval(LOAD_TIMER);
  LOAD_TIMER = setInterval(loadSummaries, 30_000);
});
