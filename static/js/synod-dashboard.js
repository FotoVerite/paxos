const tabs = document.querySelectorAll(".tab");
const panels = {
  live: document.querySelector("#livePanel"),
  controller: document.querySelector("#controllerPanel"),
  events: document.querySelector("#eventsPanel"),
};

const activeNodes = document.querySelector("#activeNodes");
const configId = document.querySelector("#configId");
const leaderId = document.querySelector("#leaderId");

tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    const target = tab.dataset.tab;
    tabs.forEach((item) => item.classList.toggle("is-active", item === tab));
    Object.entries(panels).forEach(([name, panel]) => {
      panel.classList.toggle("hidden", name !== target);
    });
  });
});

async function refreshStatus() {
  const response = await fetch("/synod/api/status");
  if (!response.ok) return;

  const status = await response.json();
  activeNodes.textContent = status.active_nodes;
  if (!status.active_configuration) {
    configId.textContent = "none";
    leaderId.textContent = "leader pending";
    return;
  }

  configId.textContent = status.active_configuration.configuration_id;
  leaderId.textContent = `leader ${status.active_configuration.leader.slice(0, 8)}`;
}

refreshStatus();
setInterval(refreshStatus, 1500);
