document.addEventListener("DOMContentLoaded", () => {

    const container = document.getElementById("selections-list");
    if (!container) return;

    // --------------------
    // Card click (except delete buttons)
    // --------------------
    container.addEventListener("click", (event) => {
        // Ignore clicks on buttons inside the card
        if (event.target.tagName === "BUTTON") return;

        // Find the closest clickable card
        const card = event.target.closest(".clickable");
        if (!card) return;

        const type = card.dataset.type;
        const id = card.dataset.id;

        handleCardClick(type, id);
    });

    function handleCardClick(type, id) {
        console.log("Card clicked:", type, id);
        if (type === "selection") {
            window.location.href = `/selections/${id}`;
        } else {
            console.error("Unknown card type:", type);
        }
    }

    // --------------------
    // Delete button click
    // --------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button");
        if (!btn) return;

        const card = btn.closest(".clickable");
        if (!card) return;

        const type = card.dataset.type;
        const id = card.dataset.id;

        handleDeleteClick(type, id, card, event);
    });

    function handleDeleteClick(type, id, cardEl, event) {
        event.stopPropagation(); // prevent triggering card click
        console.log("Delete clicked:", type, id);

        const nameText = cardEl.querySelector(".fw-bold").textContent;
        const message = `Are you sure you want to delete ${nameText}?`;

        openDeleteModal(message, () => {
            fetch(`/selections/${id}`, { method: 'DELETE' })
                .then(res => {
                    if (res.ok) {
                        cardEl.remove();
                        console.log("Deleted:", type, id);
                    } else {
                        console.error("Delete failed:", res.status);
                    }
                })
                .catch(err => console.error("Delete error:", err));
        });
    }

});
