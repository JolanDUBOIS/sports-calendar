document.addEventListener("DOMContentLoaded", () => {

    const container = document.getElementById("items-list");
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
        btn.textContent = details.classList.contains("d-none") ? "+" : "−";
    });

    // --------------------
    // Delete button click
    // --------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='delete-item']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const type = card.dataset.type;
        const selectionId = card.dataset.selectionId;
        const itemId = card.dataset.itemId;

        handleDeleteClick(type, selectionId, itemId, card);
    });

    function handleDeleteClick(type, selectionId, id, cardEl) {
        console.log("Delete clicked:", type, selectionId, id);
        const message = `Are you sure you want to delete this selection item?`;
        
        openDeleteModal(message, () => {
            fetch(`/selections/${selectionId}/items/${id}`, { method: 'DELETE' })
                .then(res => {
                    if (res.ok) {
                        cardEl.remove();
                        console.log("Deleted:", type, selectionId, id);
                    } else {
                        console.error("Failed to delete:", type, selectionId, id);
                    }
                })
                .catch(err => console.error("Error deleting:", type, selectionId, id, err));
        })
    };

    // ---------------------------
    // Modify filter button click
    // ---------------------------
    container.addEventListener("click", (event) => {
        console.log("Modify filter button clicked");
        const btn = event.target.closest("button[data-action='modify-filter']");
        if (!btn) return;

        const divEl = btn.closest(".filter-div");
        if (!divEl) return;

        const filterId = divEl.dataset.filterId;

        openModifyFilterForm(filterId);
    });

});
