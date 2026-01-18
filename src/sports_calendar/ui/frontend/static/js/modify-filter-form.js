window.openModifyFilterModal = function(filterId, onConfirm) {
    console.log("Opening modify filter modal for filterId:", filterId);
    const modalBody = document.getElementById("modify-filter-body");
    modalBody.innerHTML = ""; // clear previous content

    // clone the correct hidden template
    const template = document.getElementById(`filter-form-${filterId}`);
    if (!template) return;

    // Clone the node and remove its id
    const clone = template.cloneNode(true);
    clone.removeAttribute("id");
    clone.classList.remove("d-none");

    modalBody.appendChild(clone);

    // Initialize lookup inputs with their existing IDs from the template
    clone.querySelectorAll(".lookup-input").forEach(input => {
        if (input.dataset.initialValue !== undefined) {
            input.dataset.value = input.dataset.initialValue;
        }
    });

    // Initialize lookup fields for this form
    initLookupFields(modalBody);

    // Remove previous listeners
    const modalEl = document.getElementById("modifyFilterModal");
    const confirmBtn = modalEl.querySelector("#modify-filter-confirm-btn");
    confirmBtn.replaceWith(confirmBtn.cloneNode(true));
    const newConfirmBtn = modalEl.querySelector("#modify-filter-confirm-btn");

    // Attach new listener
    newConfirmBtn.addEventListener("click", () => {
        const form = modalBody.querySelector("form");
        if (!form) return;

        const data = {};

        form.querySelectorAll("input, select").forEach(el => {
            if (!el.name) return;

            if (el.classList.contains("lookup-input")) {
                data[el.name] = el.dataset.value ?? null;
                return;
            }

            data[el.name] = el.value;
        });

        onConfirm(data);
        bootstrap.Modal.getInstance(modalEl).hide();
    });

    new bootstrap.Modal(modalEl).show();
}