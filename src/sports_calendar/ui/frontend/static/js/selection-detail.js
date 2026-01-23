document.addEventListener("DOMContentLoaded", () => {


    const container = document.getElementById("items-list");
    const mainContainer = document.getElementById("selection-container"); // <- new main container
    console.log("Main container:", mainContainer);
    const selectionName = mainContainer ? mainContainer.dataset.selectionName : null;
    console.log("Fetched selectionName:", selectionName);

    // Always set up Create button handler, even if container is missing
    const createBtn = document.querySelector("[data-action='create-item']");
    if (createBtn) {
        createBtn.addEventListener("click", () => {
            if (!selectionName) {
                console.error("No selection id found for item creation");
                return;
            }

            openCreateItemModal((sport) => {
                if (!sport) return;
                fetch(`/selections/${selectionName}/items/`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sport })
                })
                .then(res => {
                    if (res.ok) window.location.reload();
                    else console.error("Create item failed:", res.status);
                })
                .catch(err => console.error("Create item error:", err));
            });
        });
    }

    if (!container) return;

    // ----------------------------
    // Toggle details button click
    // ----------------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='toggle-item']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const details = card.querySelector(".item-details");
        if (!details) return;

        // Toggle visibility
        details.classList.toggle("d-none");

        // Optional: change button symbol
        btn.textContent = details.classList.contains("d-none") ? "+" : "-";
    });

    // --------------------
    // Delete button click
    // --------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='delete-item']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const itemId = card.dataset.itemId;

        handleDeleteClick(selectionName, itemId, card);
    });

    function handleDeleteClick(selectionName, id, cardEl) {
        console.log("Delete clicked:", selectionName, id);
        const message = `Are you sure you want to delete this selection item?`;
        
        openDeleteModal(message, () => {
            fetch(`/selections/${selectionName}/items/${id}`, { method: 'DELETE' })
                .then(res => {
                    if (res.ok) {
                        cardEl.remove();
                        console.log("Deleted:", selectionName, id);
                    } else {
                        console.error("Failed to delete:", selectionName, id);
                    }
                })
                .catch(err => console.error("Error deleting:", selectionName, id, err));
        })
    };

    // ---------------------------
    // Modify filter button click
    // ---------------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='modify-filter']");
        if (!btn) return;

        const divEl = btn.closest(".filter-div");
        if (!divEl) return;

        const selectionName = divEl.dataset.selectionName;
        const itemId = divEl.dataset.itemId;
        const filterId = divEl.dataset.filterId;

        window.openModifyFilterModal(filterId, function(data) {
            fetch(`/selections/${selectionName}/items/${itemId}/filters/${filterId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            })
            .then(res => {
                if (res.ok) {
                    console.log("Filter updated successfully");
                } else {
                    console.error("Failed to update filter");
                }
            })
            .catch(err => console.error("Error updating filter", err));
        });
    });

    // ---------------------
    // Create Modal function
    // ---------------------
    function openCreateItemModal(onConfirm) {
        const modalEl = document.getElementById("createItemModal");
        const sportInput = modalEl.querySelector("#create-item-sport");
        const confirmBtn = modalEl.querySelector("#create-item-confirm-btn");

        sportInput.value = "";

        confirmBtn.replaceWith(confirmBtn.cloneNode(true));
        const newConfirmBtn = modalEl.querySelector("#create-item-confirm-btn");

        newConfirmBtn.addEventListener("click", () => {
            const sport = sportInput.value.trim();
            onConfirm(sport);
            bootstrap.Modal.getInstance(modalEl).hide();
        });

        const bsModal = new bootstrap.Modal(modalEl);
        bsModal.show();

        modalEl.addEventListener('shown.bs.modal', function handler() {
            sportInput.focus();
            modalEl.removeEventListener('shown.bs.modal', handler);
        });
    }

    // -----------------------
    // Add Filter button click
    // -----------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='add-filter']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const itemId = card.dataset.itemId;

        handleAddFilterClick(selectionName, itemId);
    });

    function handleAddFilterClick(selectionName, itemId) {
        console.log("Add Filter clicked:", selectionName, itemId);
        // First call 

        fetch(`/selections/${selectionName}/items/${itemId}/filters/`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        })
        .then(res => res.json())
        .then(data => {
            const newFilterId = data.uid;
            console.log("New filter created with ID:", newFilterId);

            // Open modify filter modal for the new filter
            window.openModifyFilterModal(newFilterId, function(modifyData) {
                fetch(`/selections/${selectionName}/items/${itemId}/filters/${newFilterId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(modifyData)
                })
                .then(res => {
                    if (res.ok) {
                        console.log("New filter modified successfully");
                        window.location.reload();
                    } else {
                        console.error("Failed to modify new filter");
                    }
                })
                .catch(err => console.error("Error modifying new filter", err));
            });
        })
        .catch(err => console.error("Error creating new filter", err));
    }
});
