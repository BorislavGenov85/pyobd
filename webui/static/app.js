// ---------------------------------------------------------------------
// DOM
// ---------------------------------------------------------------------

const statusEl = document.getElementById("status");
const tableBody = document.querySelector("#sensor-table tbody");

// sensor name -> DOM cells
const rowElements = {};

// ---------------------------------------------------------------------
// RPM CHART
// ---------------------------------------------------------------------

const ctx = document
    .getElementById("rpmChart")
    .getContext("2d");

const rpmChart = new Chart(ctx, {
    type: "line",

    data: {
        labels: [],
        datasets: [
            {
                label: "RPM",
                data: [],
                borderColor: "rgb(75, 192, 192)",
                tension: 0.2,
                pointRadius: 0,
            },
        ],
    },

    options: {
        animation: false,
        responsive: true,

        scales: {
            y: {
                beginAtZero: true,
            },
        },
    },
});

const MAX_POINTS = 60;


// ---------------------------------------------------------------------
// BUILD TABLE ONCE
// ---------------------------------------------------------------------

function buildTable(commands) {

    tableBody.innerHTML = "";

    for (const cmd of commands) {

        const row = document.createElement("tr");

        const sensorCell = document.createElement("td");
        const valueCell = document.createElement("td");
        const unitCell = document.createElement("td");

        // Example:
        // Engine RPM (010C)

        sensorCell.innerHTML =
            `<strong>${cmd.desc}</strong><br>` +
            `<small>${cmd.pid}</small>`;

        valueCell.textContent = "—";

        unitCell.textContent =
            cmd.unit || "";

        row.appendChild(sensorCell);
        row.appendChild(valueCell);
        row.appendChild(unitCell);

        tableBody.appendChild(row);

        rowElements[cmd.name] = {
            valueCell,
            unitCell,
        };
    }
}


// ---------------------------------------------------------------------
// UPDATE VALUES ONLY
// ---------------------------------------------------------------------

function updateValues(values) {

    for (const [name, data] of Object.entries(values)) {

        const row = rowElements[name];

        if (!row)
            continue;

        row.valueCell.textContent =
            data.value === null
                ? "—"
                : data.value;

        if (data.unit)
            row.unitCell.textContent =
                data.unit;
    }
}


// ---------------------------------------------------------------------
// RPM CHART
// ---------------------------------------------------------------------

function updateRPM(values) {

    if (!values.RPM)
        return;

    const rpm = values.RPM.value;

    if (rpm === null)
        return;

    const now =
        new Date().toLocaleTimeString();

    rpmChart.data.labels.push(now);

    rpmChart.data.datasets[0]
        .data.push(rpm);

    if (
        rpmChart.data.labels.length >
        MAX_POINTS
    ) {
        rpmChart.data.labels.shift();

        rpmChart.data.datasets[0]
            .data.shift();
    }

    rpmChart.update();
}


// ---------------------------------------------------------------------
// WEBSOCKET
// ---------------------------------------------------------------------

function connect() {

    const protocol =
        location.protocol === "https:"
            ? "wss"
            : "ws";

    const ws = new WebSocket(
        `${protocol}://${location.host}/ws/sensors`
    );

    ws.onopen = () => {

        statusEl.textContent =
            "Connected";
    };

    ws.onclose = () => {

        statusEl.textContent =
            "Disconnected - reconnecting...";

        setTimeout(
            connect,
            2000
        );
    };

    ws.onerror = () => {
        ws.close();
    };

    ws.onmessage = (event) => {

        const msg =
            JSON.parse(event.data);

        // -------------------------------------------------
        // adapter disconnected
        // -------------------------------------------------

        if (
            msg._status ===
            "disconnected"
        ) {

            statusEl.textContent =
                "OBD adapter not connected";

            return;
        }

        // -------------------------------------------------
        // capabilities packet
        // -------------------------------------------------

        if (
            msg._type ===
            "capabilities"
        ) {

            buildTable(
                msg.commands
            );

            statusEl.textContent =
                `Connected (${msg.commands.length} PIDs)`;

            return;
        }

        // -------------------------------------------------
        // live updates
        // -------------------------------------------------

        if (
            msg._type ===
            "update"
        ) {

            updateValues(
                msg.values
            );

            updateRPM(
                msg.values
            );
        }
    };
}

// =====================================================
// DTC
// =====================================================

const dtcTableBody =
    document.querySelector(
        "#dtc-table tbody"
    );

const refreshDtcBtn =
    document.getElementById(
        "refresh-dtc"
    );

const clearDtcBtn =
    document.getElementById(
        "clear-dtc"
    );


async function loadDTC() {

    dtcTableBody.innerHTML = "";

    try {

        const response =
            await fetch("/api/dtc");

        const data =
            await response.json();

        if (!data.connected) {

            const row =
                document.createElement("tr");

            row.innerHTML =
                `<td colspan="3">
            OBD adapter not connected
         </td>`;

            dtcTableBody.appendChild(
                row
            );

            return;
        }

        if (data.dtc.length === 0) {

            const row =
                document.createElement("tr");

            row.innerHTML =
                `<td colspan="3">
            No DTC codes
         </td>`;

            dtcTableBody.appendChild(
                row
            );

            return;
        }

        for (const dtc of data.dtc) {

            const row =
                document.createElement("tr");

            row.innerHTML = `
        <td>${dtc.code}</td>
        <td class="dtc-active">
            ${dtc.status}
        </td>
        <td>${dtc.description}</td>
      `;

            dtcTableBody.appendChild(
                row
            );
        }

    } catch (err) {

        console.error(err);
    }
}

async function loadVehicleTests() {
    const tableBody = document.querySelector('#tests-table tbody') || document.createElement('tbody');
    if (!document.querySelector('#tests-table tbody')) {
        document.getElementById('tests-table').appendChild(tableBody);
    }

    tableBody.innerHTML = `<tr><td colspan="2" style="text-align:center; color:gray;">Loading system tests...</td></tr>`;

    try {
        const response = await fetch('/api/tests');
        const data = await response.json();

        if (data.status !== 'success' || !data.tests || data.tests.length === 0) {
            tableBody.innerHTML = `<tr><td colspan="2" style="text-align:center; color:red;">No diagnostic data available. Make sure OBD is connected.</td></tr>`;
            return;
        }

        const engineBadge = document.getElementById('engine-type-badge');
        if (engineBadge && data.engine_type) {
            engineBadge.innerHTML = `Engine Type: <span class="badge ${data.engine_type.includes('Бензин') ? 'badge-spark' : 'badge-diesel'}">${data.engine_type}</span>`;
        }

        tableBody.innerHTML = '';

        data.tests.forEach(test => {
            const row = document.createElement('tr');

            const formattedName = test.description
                .toLowerCase()
                .replace(/_/g, ' ')
                .replace(/\b\w/g, c => c.toUpperCase());

            let statusHtml = '';
            if (test.available === 'Available') {
                if (test.complete === 'Complete') {
                    statusHtml = `<span class="badge-ok">✅ Complete</span>`;
                } else {
                    statusHtml = `<span class="badge-warning">⚠️ Incomplete</span>`;
                }
            } else {
                statusHtml = `<span style="color: gray; font-weight: 700;">✖ Not Supported</span>`;
            }

            row.innerHTML = `
                <td><strong>${formattedName}</strong></td>
                <td>${statusHtml}</td>
            `;

            tableBody.appendChild(row);
        });

    } catch (error) {
        console.error('Error loading tests:', error);
        tableBody.innerHTML = `<tr><td colspan="2" style="text-align:center; color:red;">Server error connection failed.</td></tr>`;
    }
}


refreshDtcBtn.addEventListener(
    "click",
    loadDTC
);

clearDtcBtn.addEventListener(
    "click",
    async () => {

        const ok = confirm(
            "Clear all DTC codes?\n\n" +
            "This will also clear " +
            "freeze frame data."
        );

        if (!ok)
            return;

        try {

            const response =
                await fetch(
                    "/api/dtc/clear",
                    {
                        method: "POST"
                    }
                );

            const result =
                await response.json();

            if (result.success) {

                alert(
                    "DTC cleared."
                );

                await loadDTC();

            } else {

                alert(
                    result.error ||
                    "Failed to clear DTC"
                );
            }

        } catch (err) {

            alert(err);
        }
    }
);

const freezeTableBody = document.querySelector("#freeze-table tbody");

async function loadFreeze() {
    freezeTableBody.innerHTML = "";
    try {
        const response = await fetch("/api/freeze");

        const data = await response.json();

        if (!data.connected) {
            freezeTableBody.innerHTML =
                `<tr>
            <td colspan="2">
              OBD adapter not connected
            </td>
         </tr>`;

            return;
        }

        if (!data.supported) {
            freezeTableBody.innerHTML =
                `<tr>
            <td colspan="2">
              No freeze frame data available
            </td>
         </tr>`;

            return;
        }

        if (data.freeze_dtc) {
            const row = document.createElement("tr");
            row.innerHTML =
                `<td>Trigger DTC</td>
         <td>${data.freeze_dtc}</td>`;

            freezeTableBody.appendChild(
                row
            );
        }

    } catch (err) {

        console.error(err);
    }
}

// start-stop btn logic
const logBtn = document.getElementById("start-log");

let logging = false;

logBtn.addEventListener("click", async () => {
        const url = logging
            ? "/api/logging/stop"
            : "/api/logging/start";

        const response = await fetch(url, {method: "POST"});
        const data = await response.json();

        logging = data.logging;
        if (logging) {
            logBtn.textContent =
                "Stop Logging";
        } else {
            logBtn.textContent =
                "Start Logging";
        }
    }
);

document.querySelectorAll(".tab-button").forEach(btn => {
    btn.addEventListener("click", () => {
            // remove "active" from all buttons
            document.querySelectorAll(".tab-button").forEach(b => b.classList.remove("active"));
            // hide all tabs
            document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
            // activate button
            btn.classList.add("active");
            const tab = btn.dataset.tab;
            // show current tab
            document.getElementById(tab).classList.add("active");
            // export button
            document.getElementById("export-current")
                .addEventListener("click", async function() {const btn = this;
                    if (btn.disabled) return;

                    btn.disabled = true;
                    const originalText = btn.innerHTML;
                    btn.innerHTML = "⏳ Exporting...";

                    try {
                        const response = await fetch('/api/export/current');
                        if (!response.ok) throw new Error('Network response was not ok');

                        const blob = await response.blob();
                        const url = window.URL.createObjectURL(blob);

                        const a = document.createElement('a');
                        a.href = url;

                        const timestamp = new Date().toISOString().slice(0, 19).replace(/T|:/g, "_");
                        a.download = `snapshot_${timestamp}.csv`;

                        document.body.appendChild(a);
                        a.click();

                        document.body.removeChild(a);
                        window.URL.revokeObjectURL(url);

                    } catch (error) {
                        console.error('Export error:', error);
                        alert('Грешка при експорт на данни! Проверете OBD връзката.');
                    } finally {
                        btn.disabled = false;
                        btn.innerHTML = originalText;
                    }
                });
            // test btn
            document.getElementById('tab-btn-tests')?.addEventListener('click', () => {
                window.location = "/api/tests"
            });

            switch (tab) {

                case "dtc":
                    loadDTC();
                    break;

                case "freeze":
                    loadFreeze();
                    break;

                case "tests":
                    loadVehicleTests();
                    break
            }
        }
    );
});

// ---------------------------------------------------------------------
// START
// ---------------------------------------------------------------------

connect();