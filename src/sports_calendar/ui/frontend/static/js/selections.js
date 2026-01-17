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
        const name = card.dataset.name;

        handleCardClick(type, name);
    });

    function handleCardClick(type, name) {
        console.log("Card clicked:", type, name);
        if (type === "selection") {
            window.location.href = `/selections/${name}`;
        } else {
            console.error("Unknown card type:", type);
        }
    }

    // --------------------
    // Delete button click
    // --------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='delete-selection']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const type = card.dataset.type;
        const name = card.dataset.name;

        handleDeleteClick(type, name, card, event);
    });

    function handleDeleteClick(type, name, cardEl, event) {
        event.stopPropagation(); // prevent triggering card click
        console.log("Delete clicked:", type, name);

        const nameText = cardEl.querySelector(".fw-bold").textContent;
        const message = `Are you sure you want to delete ${nameText}?`;

        openDeleteModal(message, () => {
            fetch(`/selections/${name}`, { method: 'DELETE' })
                .then(res => {
                    if (res.ok) {
                        cardEl.remove();
                        console.log("Deleted:", type, name);
                    } else {
                        console.error("Delete failed:", res.status);
                    }
                })
                .catch(err => console.error("Delete error:", err));
        });
    }
});
