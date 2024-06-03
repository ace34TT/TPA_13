document
  .getElementById("customerForm")
  .addEventListener("submit", function (e) {
    e.preventDefault();

    const formData = new FormData(this);
    const formObject = Object.fromEntries(formData.entries());
    console.log(formObject);
    fetch("http://127.0.0.1:5662/predict_marketing", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(formObject),
    })
      .then((response) => response.json())
      .then((data) => {
        console.log(data);
        document.getElementById("response").innerText = data[0].categorie;
      })
      .catch((error) => console.error("Error:", error));
  });
document.addEventListener("DOMContentLoaded", function () {
  fetchDataAndFillTable();
});

function fetchDataAndFillTable() {
  fetch("http://127.0.0.1:5662/get_marketing")
    .then((response) => response.json())
    .then((data) => {
      console.log(data);
      const tableBody = document.querySelector("#dataTable tbody");
      data.forEach((item) => {
        const row = document.createElement("tr");
        // row.classList.add("bg-white border-b");
        row.innerHTML = `
              <td scope="row"
                class="px-6 py-4 font-medium text-gray-900 whitespace-nowrap"
                >${item.ID}</td>
              <td class="px-6 py-4">${item.AGE}</td>
              <td class="px-6 py-4">${item.SEXE}</td>
              <td class="px-6 py-4">${item.TAUX}</td>
              <td class="px-6 py-4">${item.SITUATIONFAMILIALE}</td>
              <td class="px-6 py-4">${item.NBENFANTSACHARGE}</td>
              <td class="px-6 py-4">${item.DEUXIEMEVOITURE ? "Oui" : "Non"}</td>
              <td class="px-6 py-4">${item.CATEGORIE}</td>
            `;
        tableBody.appendChild(row);
      });
    })
    .catch((error) => console.error("Error:", error));
}
