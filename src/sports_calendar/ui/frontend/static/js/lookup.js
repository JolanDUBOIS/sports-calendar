function initLookupFields(container = document) {
    console.log("Initializing lookup fields in container:", container);
    container.querySelectorAll(".lookup-field").forEach(fieldContainer => {
        const input = fieldContainer.querySelector(".lookup-input");
        const suggestionsEl = fieldContainer.querySelector(".lookup-suggestions");
        const endpoint = fieldContainer.dataset.endpoint;

        input.addEventListener("input", async () => {
            const query = input.value.trim();
            if (!query) {
                suggestionsEl.classList.add("d-none");
                suggestionsEl.innerHTML = "";
                return;
            }

            const res = await fetch(`${endpoint}?query=${encodeURIComponent(query)}`);
            const data = await res.json();
            renderSuggestions(suggestionsEl, data, input);
        });
    });
}

function renderSuggestions(listEl, data, inputEl) {
    console.log("Rendering suggestions:", data);
    listEl.innerHTML = "";
    const key = Object.keys(data)[0]; // e.g., "teams" or "competitions"
    const options = data[key] || [];

    if (!options.length) {
        listEl.classList.add("d-none");
        return;
    }

    options.forEach(opt => {
        const li = document.createElement("li");
        li.className = "list-group-item list-group-item-action";
        li.textContent = opt.display;
        li.dataset.value = opt.value;

        li.addEventListener("click", () => {
            inputEl.value = opt.display;      // for user display
            inputEl.dataset.value = opt.value; // actual value to send
            listEl.classList.add("d-none");
        });

        listEl.appendChild(li);
    });

    listEl.classList.remove("d-none");
}
