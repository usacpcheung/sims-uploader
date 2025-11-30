(function () {
  const form = document.getElementById('preview-form');
  const previewState = document.getElementById('preview-state');
  const previewResults = document.getElementById('preview-results');
  const detectedList = document.querySelector('[data-detected-headers]');
  const metadataList = document.querySelector('[data-metadata-list]');
  const collisionWarning = document.querySelector('[data-collision-warning]');
  const collisionList = document.querySelector('[data-collision-list]');
  const mappingBody = document.querySelector('[data-mapping-body]');
  const validationBox = document.querySelector('[data-validation-messages]');
  const validateButton = document.querySelector('[data-validate]');
  const uploadHint = document.querySelector('[data-upload-hint]');
  const emptyRow = document.querySelector('[data-empty-row]');

  let lastPreview = null;

  function setPreviewState(message, isError = false) {
    if (!previewState) return;
    previewState.textContent = message;
    previewState.classList.toggle('table-state--error', Boolean(isError));
    previewState.hidden = false;
  }

  function renderPills(target, items) {
    if (!target) return;
    target.innerHTML = '';
    items.forEach((item) => {
      const pill = document.createElement('li');
      pill.textContent = item;
      target.appendChild(pill);
    });
  }

  function renderMappings(preview) {
    if (!mappingBody) return;
    mappingBody.innerHTML = '';

    const headers = preview.headers || [];
    const required = new Set(preview.suggested_required_columns || []);
    const mappings = preview.suggested_column_mappings || {};
    const mappingKeys = Object.keys(mappings);

    if (!mappingKeys.length) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 3;
      cell.className = 'table-state table-state--empty';
      cell.textContent = 'No data columns were detected in this sheet.';
      row.appendChild(cell);
      mappingBody.appendChild(row);
      return;
    }

    mappingKeys.forEach((targetName) => {
      const row = document.createElement('tr');

      const targetCell = document.createElement('td');
      const targetInput = document.createElement('input');
      targetInput.type = 'text';
      targetInput.value = targetName;
      targetInput.name = `target-${targetName}`;
      targetCell.appendChild(targetInput);

      const sourceCell = document.createElement('td');
      const select = document.createElement('select');
      const emptyOption = document.createElement('option');
      emptyOption.value = '';
      emptyOption.textContent = '— choose header —';
      select.appendChild(emptyOption);

      headers.forEach((header) => {
        const option = document.createElement('option');
        option.value = header;
        option.textContent = header;
        if (mappings[targetName] === header) {
          option.selected = true;
        }
        select.appendChild(option);
      });

      sourceCell.appendChild(select);

      const requiredCell = document.createElement('td');
      const requiredToggle = document.createElement('input');
      requiredToggle.type = 'checkbox';
      requiredToggle.checked = required.has(targetName);
      requiredCell.appendChild(requiredToggle);

      row.appendChild(targetCell);
      row.appendChild(sourceCell);
      row.appendChild(requiredCell);
      mappingBody.appendChild(row);
    });
  }

  function renderPreview(preview) {
    lastPreview = preview;
    if (emptyRow) {
      emptyRow.hidden = true;
    }
    setPreviewState(`Previewed ${preview.headers.length} headers from ${preview.sheet}.`);
    if (previewResults) {
      previewResults.hidden = false;
    }
    renderPills(detectedList, preview.headers || []);
    renderPills(metadataList, preview.metadata_columns || []);

    if (collisionWarning) {
      const collisions = preview.metadata_collisions || [];
      collisionWarning.hidden = collisions.length === 0;
      if (collisionList) {
        collisionList.innerHTML = '';
        collisions.forEach((item) => {
          const pill = document.createElement('div');
          pill.textContent = item;
          collisionList.appendChild(pill);
        });
      }
    }

    if (validationBox) {
      validationBox.hidden = true;
      validationBox.textContent = '';
    }

    renderMappings(preview);
  }

  function collectMappings() {
    if (!mappingBody) return [];
    const rows = mappingBody.querySelectorAll('tr');
    return Array.from(rows).map((row) => {
      const inputs = row.querySelectorAll('input');
      const select = row.querySelector('select');
      const targetInput = inputs[0];
      const requiredToggle = inputs[1];
      return {
        target: targetInput ? targetInput.value.trim() : '',
        source: select ? select.value : '',
        required: requiredToggle ? requiredToggle.checked : false
      };
    });
  }

  function validateMappings() {
    if (!lastPreview || !validationBox) return;

    const metadataSet = new Set(lastPreview.metadata_columns || []);
    const headers = new Set(lastPreview.headers || []);
    const seenTargets = new Set();
    const rows = collectMappings();
    const errors = [];

    rows.forEach((row) => {
      if (!row.target) {
        errors.push('All mapping rows need a target column name.');
        return;
      }
      if (seenTargets.has(row.target)) {
        errors.push(`Duplicate target column detected: ${row.target}`);
      }
      seenTargets.add(row.target);

      if (metadataSet.has(row.target)) {
        errors.push(`Target column ${row.target} collides with reserved metadata.`);
      }

      if (row.required && !row.source) {
        errors.push(`Required column ${row.target} is not linked to a source header.`);
      }

      if (row.source && !headers.has(row.source)) {
        errors.push(`Source header ${row.source} is not present in the preview.`);
      }
    });

    const missingRequired = (lastPreview.suggested_required_columns || []).filter(
      (column) => !rows.some((row) => row.target === column && row.source)
    );

    missingRequired.forEach((column) => {
      errors.push(`Required column ${column} is missing a mapped header.`);
    });

    validationBox.hidden = false;
    if (!errors.length) {
      validationBox.classList.remove('table-state--error');
      validationBox.textContent = 'All required columns are mapped and free of metadata collisions.';
      return;
    }

    validationBox.classList.add('table-state--error');
    validationBox.innerHTML = errors
      .map((message) => `<div>${message}</div>`)
      .join('');
  }

  async function submitPreview(event) {
    event.preventDefault();
    if (!form) return;

    const formData = new FormData(form);
    const fileField = form.querySelector('input[type="file"]');
    if (!fileField || !fileField.files.length) {
      setPreviewState('Please choose a workbook file to preview.', true);
      return;
    }

    setPreviewState('Uploading workbook and detecting headers…');

    try {
      const response = await fetch('/config/preview', {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({ detail: 'Unknown error' }));
        const message = payload.detail || 'Unable to preview workbook.';
        setPreviewState(message, true);
        return;
      }

      const preview = await response.json();
      renderPreview(preview);
    } catch (error) {
      setPreviewState('Preview failed. Please try again with a smaller sample.', true);
    }
  }

  if (form) {
    form.addEventListener('submit', submitPreview);
  }

  if (validateButton) {
    validateButton.addEventListener('click', validateMappings);
  }

  if (uploadHint && form) {
    const fileInput = form.querySelector('input[type="file"]');
    if (fileInput) {
      fileInput.addEventListener('change', () => {
        if (!fileInput.files.length) {
          uploadHint.textContent = 'Waiting for a workbook sample…';
          return;
        }
        uploadHint.textContent = `Ready to preview: ${fileInput.files[0].name}`;
      });
    }
  }
})();
