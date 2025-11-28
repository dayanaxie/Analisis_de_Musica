const statusContainer = document.getElementById("loader-status");
const btnRun = document.getElementById("btn-run-loader");
const btnRefresh = document.getElementById("btn-refresh-status");

const analysisStatusContainer = document.getElementById("analysis-status");
const btnRunAnalysis = document.getElementById("btn-run-analysis");

function renderStatusBox({ type }) {
  if (!statusContainer) return;

  let label = "Estado";
  let heading = "Loader listo";
  let text = "Use el botón para iniciar la carga de datos.";
  let className = "status-idle";
  let extra = "";

  if (type === "running") {
    label = "En curso";
    heading = "Ejecución del loader";
    text =
      "El proceso está corriendo. Este panel se actualizará cuando cambie el estado.";
    className = "status-running";
    extra = `<div class="spinner" aria-label="Cargando"></div>`;
  } else if (type === "completed") {
    label = "Finalizado";
    heading = "Loader completado";
    text =
      "La carga de datos terminó correctamente. Puede volver a ejecutarla si lo necesita.";
    className = "status-completed";
  }

  statusContainer.innerHTML = `
    <div class="status-box ${className}">
      <div>
        <div class="status-label">${label}</div>
        <div class="status-heading">${heading}</div>
        <div class="status-text">${text}</div>
      </div>
      <div></div>
      ${extra}
    </div>
  `;
}

function updateStatus() {
  if (!statusContainer) return;

  renderStatusBox({ type: "running" });

  fetch("/admin/loader-status")
    .then((response) => response.json())
    .then((data) => {
      if (data.running === true) {
        renderStatusBox({ type: "running" });
      } else if (data.status === "completed") {
        renderStatusBox({ type: "completed" });
      } else {
        renderStatusBox({ type: "idle" });
      }
    })
    .catch(() => {
      renderStatusBox({ type: "idle" });
    });
}

function runLoader() {
  if (!statusContainer) return;

  renderStatusBox({ type: "running" });

  fetch("/admin/load-data", {
    method: "POST",
  })
    .then((response) => response.json())
    .then((data) => {
      alert(data.message || "Loader ejecutado.");
      updateStatus();
    })
    .catch((err) => {
      alert("Error ejecutando el loader: " + err);
      updateStatus();
    });
}

function renderAnalysisStatus(type, customMessage) {
  if (!analysisStatusContainer) return;

  let label = "Estado";
  let heading = "Análisis listo";
  let text = "Use el botón para lanzar el análisis global con Spark.";
  let className = "status-idle";
  let extra = "";

  if (type === "running") {
    label = "En curso";
    heading = "Ejecutando análisis";
    text =
      "El job de Spark se está ejecutando. Esto puede tardar algunos segundos.";
    className = "status-running";
    extra = `<div class="spinner" aria-label="Cargando"></div>`;
  } else if (type === "completed") {
    label = "Finalizado";
    heading = "Análisis completado";
    text =
      customMessage ||
      "El análisis terminó correctamente y los resultados se guardaron en la base de datos.";
    className = "status-completed";
  } else if (type === "error") {
    label = "Error";
    heading = "Fallo al ejecutar el análisis";
    text = customMessage || "Revise los logs del servidor para más detalles.";
    className = "status-idle";
  }

  analysisStatusContainer.innerHTML = `
    <div class="status-box ${className}">
      <div>
        <div class="status-label">${label}</div>
        <div class="status-heading">${heading}</div>
        <div class="status-text">${text}</div>
      </div>
      <div></div>
      ${extra}
    </div>
  `;
}

function runAnalysis() {
  if (!analysisStatusContainer) return;

  renderAnalysisStatus("running");

  fetch("/admin/run-analysis", {
    method: "POST",
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.status === "ok" || data.success === true) {
        renderAnalysisStatus("completed", data.message);
      } else {
        renderAnalysisStatus(
          "error",
          data.message || "Error desconocido ejecutando el análisis."
        );
      }
    })
    .catch((err) => {
      renderAnalysisStatus(
        "error",
        "Error ejecutando el análisis: " + err
      );
    });
}

// Loader
if (btnRun) {
  btnRun.addEventListener("click", runLoader);
}
if (btnRefresh) {
  btnRefresh.addEventListener("click", updateStatus);
}
if (statusContainer) {
  updateStatus();
}

// Análisis
if (btnRunAnalysis) {
  btnRunAnalysis.addEventListener("click", runAnalysis);
}
if (analysisStatusContainer) {
  renderAnalysisStatus("idle");
}
