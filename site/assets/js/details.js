/* eslint-disable no-undef */
const openDetailsOnLoad = function openDetailsOnLoad() {
  const target = window.location.hash;
  let qTarget = null;
  if (target) {
    qTarget = document.querySelector(target);
  }

  if (!qTarget) { return; }
  let qDetails = qTarget.closest("details");
  while (qDetails) {
    if (qDetails.matches("details")) { qDetails.open = true; }
    qDetails = qDetails.parentElement;
  }

  // Scroll to the target element
  qTarget.scrollIntoView();
};

window.addEventListener("load", openDetailsOnLoad);
