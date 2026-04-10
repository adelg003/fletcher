"use strict";

(function () {
  const instances = new Map();
  const MIN_SCALE = 0.5;
  const MAX_SCALE = 1.6;

  function getNodeRecord(widgetId) {
    return instances.get(widgetId) || null;
  }

  function destroyRecord(record) {
    if (!record) return;
    if (record.resizeObserver && typeof record.resizeObserver.disconnect === "function") {
      record.resizeObserver.disconnect();
    }
    if (record.dag && typeof record.dag.destory === "function") {
      record.dag.destory();
    }
  }

  function clampScale(scale) {
    return Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale));
  }

  function updateZoomLabel(record) {
    if (!record || !record.widget) return;
    const label = record.widget.querySelector("[data-role='zoom-label']");
    if (label) {
      label.textContent = `${Math.round((record.scale || 1) * 100)}%`;
    }
  }

  function centerDag(record) {
    if (!record || !record.dag || !record.canvas) return;
    record.dag.center({
      width: Math.max(record.canvas.clientWidth || 0, 320),
      height: Math.max(record.canvas.clientHeight || 0, 320),
    });
  }

  function setDagScale(record, nextScale) {
    if (!record || !record.dag) return;
    record.scale = clampScale(nextScale);
    record.dag.setScale(record.scale);
    centerDag(record);
    updateZoomLabel(record);
  }

  function sizeCanvas(canvas, nodes) {
    if (!canvas) return 320;
    const nodeHeight = Math.max(320, (Array.isArray(nodes) ? nodes.length : 0) * 110);
    const viewportHeight = Math.max(
      320,
      window.innerHeight - canvas.getBoundingClientRect().top - 40,
    );
    const targetHeight = Math.max(320, Math.min(nodeHeight, viewportHeight));
    canvas.style.height = `${targetHeight}px`;
    return targetHeight;
  }

  function fitDag(record) {
    if (!record || !record.dag || !record.canvas) return;
    const content = record.dag.getRootContentElement();
    if (!content) return;
    const currentScale = record.scale || 1;
    const measuredWidth =
      Math.max(content.scrollWidth || 0, content.getBoundingClientRect().width || 0, 1);
    const measuredHeight =
      Math.max(content.scrollHeight || 0, content.getBoundingClientRect().height || 0, 1);
    const naturalWidth = measuredWidth / currentScale;
    const naturalHeight = measuredHeight / currentScale;
    const availableWidth = Math.max(record.canvas.clientWidth - 32, 240);
    const availableHeight = Math.max(record.canvas.clientHeight - 32, 240);
    const nextScale = clampScale(
      Math.min(availableWidth / naturalWidth, availableHeight / naturalHeight, 1),
    );
    setDagScale(record, nextScale);
  }

  function stateColors(state) {
    switch (state) {
      case "success":
        return ["#22c55e", "rgba(34, 197, 94, 0.12)", "#166534", "#dcfce7"];
      case "running":
        return ["#38bdf8", "rgba(56, 189, 248, 0.12)", "#075985", "#e0f2fe"];
      case "failed":
        return ["#ef4444", "rgba(239, 68, 68, 0.12)", "#991b1b", "#fee2e2"];
      case "queued":
        return ["#6b7280", "rgba(107, 114, 128, 0.12)", "#374151", "#f3f4f6"];
      case "defined":
        return ["#d97706", "rgba(245, 158, 11, 0.12)", "#92400e", "#fef3c7"];
      case "waiting":
      default:
        return ["#9ca3af", "rgba(156, 163, 175, 0.12)", "#4b5563", "#f3f4f6"];
    }
  }

  function applyNodeElementState(wrapper, node, selectedNodeId) {
    const data = node.data || {};
    const [border, background, badgeColor, badgeBackground] = stateColors(data.state);
    const isSelected = selectedNodeId && node.id === selectedNodeId;
    wrapper.dataset.fletcherNodeId = node.id;
    wrapper.style.border = `2px solid ${border}`;
    wrapper.style.background = background;
    wrapper.style.boxShadow = isSelected
      ? "0 0 0 3px rgba(251, 191, 36, 0.45), 0 10px 24px rgba(15, 23, 42, 0.16)"
      : "0 6px 18px rgba(15, 23, 42, 0.08)";
    const title = wrapper.querySelector("[data-role='title']");
    const version = wrapper.querySelector("[data-role='version']");
    const badge = wrapper.querySelector("[data-role='badge']");
    const compute = wrapper.querySelector("[data-role='compute']");
    const run = wrapper.querySelector("[data-role='run']");
    if (title) title.textContent = data.name || node.id;
    if (version) version.textContent = data.version || "";
    if (compute) compute.textContent = `Compute: ${data.compute || "-"}`;
    if (badge) {
      badge.textContent = data.state || "unknown";
      badge.style.background = badgeBackground;
      badge.style.color = badgeColor;
    }
    if (run) {
      if (data.run_id) {
        run.textContent = `Run: ${data.run_id.slice(0, 8)}…`;
        run.style.display = "block";
      } else {
        run.textContent = "";
        run.style.display = "none";
      }
    }
  }

  function createNodeElement(node, selectedNodeId, selectionInfo) {
    const data = node.data || {};

    const wrapper = document.createElement("button");
    wrapper.type = "button";
    wrapper.style.width = "220px";
    wrapper.style.minHeight = "88px";
    wrapper.style.boxSizing = "border-box";
    wrapper.style.borderRadius = "12px";
    wrapper.style.padding = "12px";
    wrapper.style.display = "flex";
    wrapper.style.flexDirection = "column";
    wrapper.style.gap = "8px";
    wrapper.style.fontFamily =
      "ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
    wrapper.style.textAlign = "left";
    wrapper.style.cursor = selectionInfo ? "pointer" : "default";

    const top = document.createElement("div");
    top.style.display = "flex";
    top.style.justifyContent = "space-between";
    top.style.gap = "8px";
    top.style.alignItems = "flex-start";

    const titleBlock = document.createElement("div");

    const title = document.createElement("div");
    title.dataset.role = "title";
    title.style.fontWeight = "700";
    title.style.color = "#111827";
    title.style.lineHeight = "1.2";
    title.textContent = data.name || node.id;

    const version = document.createElement("div");
    version.dataset.role = "version";
    version.style.fontSize = "12px";
    version.style.opacity = "0.7";
    version.textContent = data.version || "";

    titleBlock.appendChild(title);
    titleBlock.appendChild(version);

    const badge = document.createElement("span");
    badge.dataset.role = "badge";
    badge.textContent = data.state || "unknown";
    badge.style.alignSelf = "flex-start";
    badge.style.padding = "2px 8px";
    badge.style.borderRadius = "9999px";
    badge.style.fontSize = "12px";
    badge.style.fontWeight = "700";
    badge.style.textTransform = "uppercase";

    top.appendChild(titleBlock);
    top.appendChild(badge);

    const meta = document.createElement("div");
    meta.style.fontSize = "12px";
    meta.style.color = "#4b5563";
    meta.style.display = "flex";
    meta.style.flexDirection = "column";
    meta.style.gap = "4px";

    const compute = document.createElement("div");
    compute.dataset.role = "compute";
    compute.textContent = `Compute: ${data.compute || "-"}`;
    meta.appendChild(compute);

    const run = document.createElement("div");
    run.dataset.role = "run";
    meta.appendChild(run);

    if (selectionInfo) {
      wrapper.addEventListener("click", function () {
        window.dispatchEvent(
          new CustomEvent("fletcher:dag-step-selected", {
            detail: {
              datasetId: selectionInfo.datasetId,
              planRunId: selectionInfo.planRunId,
              dataProductId: node.id,
            },
          }),
        );
      });
    } else {
      wrapper.disabled = true;
    }

    wrapper.appendChild(top);
    wrapper.appendChild(meta);
    applyNodeElementState(wrapper, node, selectedNodeId);
    return wrapper;
  }

  function updatePlanDagSelection(widgetId, selectedNodeId) {
    const record = getNodeRecord(widgetId);
    const widget = document.getElementById(widgetId);
    if (!record || !widget) return;
    record.selectedNodeId = selectedNodeId || "";
    record.nodes.forEach(function (node, nodeId) {
      const element = widget.querySelector(`[data-fletcher-node-id="${nodeId}"]`);
      if (element) applyNodeElementState(element, node, record.selectedNodeId);
    });
  }

  function updatePlanDagWidgetNodes(widgetId, nodes) {
    const record = getNodeRecord(widgetId);
    const widget = document.getElementById(widgetId);
    if (!record || !widget || !Array.isArray(nodes)) return;
    record.nodes = new Map(nodes.map(function (node) {
      return [node.id, node];
    }));
    sizeCanvas(record.canvas, nodes);
    nodes.forEach(function (node) {
      const element = widget.querySelector(`[data-fletcher-node-id="${node.id}"]`);
      if (element) applyNodeElementState(element, node, record.selectedNodeId);
    });
  }

  function installPanBehavior(record) {
    const canvas = record.canvas;
    if (!canvas || canvas.dataset.fletcherPanBound === "true") return;
    canvas.dataset.fletcherPanBound = "true";

    let dragging = false;
    let startX = 0;
    let startY = 0;
    let scrollLeft = 0;
    let scrollTop = 0;

    canvas.addEventListener("mousedown", function (event) {
      if (event.button !== 0) return;
      if (event.target && event.target.closest("[data-fletcher-node-id]")) return;
      dragging = true;
      startX = event.clientX;
      startY = event.clientY;
      scrollLeft = canvas.scrollLeft;
      scrollTop = canvas.scrollTop;
      canvas.style.cursor = "grabbing";
      event.preventDefault();
    });

    window.addEventListener("mousemove", function (event) {
      if (!dragging) return;
      canvas.scrollLeft = scrollLeft - (event.clientX - startX);
      canvas.scrollTop = scrollTop - (event.clientY - startY);
    });

    window.addEventListener("mouseup", function () {
      dragging = false;
      canvas.style.cursor = "grab";
    });
  }

  function installControlBehavior(record) {
    const widget = record.widget;
    if (!widget || widget.dataset.fletcherControlsBound === "true") return;
    widget.dataset.fletcherControlsBound = "true";

    widget.addEventListener("click", function (event) {
      const action = event.target && event.target.dataset
        ? event.target.dataset.planAction
        : null;
      if (!action) return;
      if (action === "zoom-in") setDagScale(record, (record.scale || 1) + 0.1);
      if (action === "zoom-out") setDagScale(record, (record.scale || 1) - 0.1);
      if (action === "fit") fitDag(record);
    });

    record.canvas.addEventListener("wheel", function (event) {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      const delta = event.deltaY < 0 ? 0.1 : -0.1;
      setDagScale(record, (record.scale || 1) + delta);
    }, { passive: false });
  }

  function installResizeBehavior(record) {
    if (!record || !record.widget || record.widget.dataset.fletcherResizeBound === "true") return;
    record.widget.dataset.fletcherResizeBound = "true";

    const resizeHandler = function () {
      sizeCanvas(record.canvas, Array.from(record.nodes.values()));
      fitDag(record);
    };

    if (window.ResizeObserver) {
      record.resizeObserver = new ResizeObserver(resizeHandler);
      record.resizeObserver.observe(record.widget);
    }
    window.addEventListener("resize", resizeHandler);
  }

  function renderDagWidget(widget) {
    if (!widget || !window.niceDag) return;

    const canvas = widget.querySelector(".fletcher-dag-canvas");
    const dataScript = widget.querySelector(".fletcher-dag-data");
    if (!canvas || !dataScript) return;

    const widgetId = widget.id || canvas.id || Math.random().toString(16).slice(2);
    const existing = instances.get(widgetId);
    if (existing && existing.widget === widget) {
      return;
    }
    if (existing) {
      destroyRecord(existing);
      instances.delete(widgetId);
    }

    const nodes = JSON.parse(dataScript.textContent || "[]");
    const selectedNodeId = widget.dataset.selectedNodeId || "";
    const selectionInfo =
      widget.dataset.datasetId && widget.dataset.planRunId
        ? {
            datasetId: widget.dataset.datasetId,
            planRunId: widget.dataset.planRunId,
          }
        : null;

    canvas.innerHTML = "";
    canvas.style.position = "relative";
    canvas.style.width = "100%";
    canvas.style.cursor = "grab";
    sizeCanvas(canvas, nodes);

    if (!Array.isArray(nodes) || nodes.length === 0) {
      const empty = document.createElement("div");
      empty.textContent = "No plan steps to display.";
      empty.style.padding = "1rem";
      empty.style.borderRadius = "12px";
      empty.style.background = "rgba(148, 163, 184, 0.12)";
      empty.style.color = "#475569";
      canvas.appendChild(empty);
      return;
    }

    try {
      const dag = window.niceDag.init({
        container: canvas,
        mode: "DEFAULT",
        omitJointBeforeEnd: true,
        graphLabel: {
          rankdir: "LR",
          nodesep: 40,
          ranksep: 70,
        },
        rootViewPadding: {
          top: 20,
          right: 24,
          bottom: 20,
          left: 24,
        },
        getNodeSize: function getNodeSize() {
          return { width: 220, height: 88 };
        },
        getEdgeAttributes: function getEdgeAttributes() {
          return { color: "#94a3b8" };
        },
        mapNodeToElement: function mapNodeToElement(node) {
          return createNodeElement(node, selectedNodeId, selectionInfo);
        },
      });

      dag.withNodes(nodes).render();
      const record = {
        widget: widget,
        canvas: canvas,
        dag: dag,
        nodes: new Map(nodes.map(function (node) {
          return [node.id, node];
        })),
        selectedNodeId: selectedNodeId,
        scale: 1,
      };
      instances.set(widgetId, record);
      installPanBehavior(record);
      installControlBehavior(record);
      installResizeBehavior(record);
      requestAnimationFrame(function () {
        fitDag(record);
      });
    } catch (error) {
      console.error("Failed to render Fletcher DAG widget", error);
      const failure = document.createElement("div");
      failure.textContent = "Failed to render plan.";
      failure.style.padding = "1rem";
      failure.style.borderRadius = "12px";
      failure.style.background = "rgba(239, 68, 68, 0.12)";
      failure.style.color = "#991b1b";
      canvas.appendChild(failure);
    }
  }

  function initPlanDagWidgets(root) {
    const scope = root && root.querySelectorAll ? root : document;
    const widgets = [];
    if (scope.matches && scope.matches(".fletcher-dag-widget")) {
      widgets.push(scope);
    }
    scope.querySelectorAll &&
      scope
        .querySelectorAll(".fletcher-dag-widget")
        .forEach(function (widget) {
          widgets.push(widget);
        });
    widgets.forEach(renderDagWidget);
  }

  window.initPlanDagWidgets = initPlanDagWidgets;
  window.updatePlanDagSelection = updatePlanDagSelection;
  window.updatePlanDagWidgetNodes = updatePlanDagWidgetNodes;

  document.addEventListener("DOMContentLoaded", function () {
    initPlanDagWidgets(document);
  });

  window.addEventListener("load", function () {
    initPlanDagWidgets(document);
  });

  document.body.addEventListener("htmx:afterSwap", function (event) {
    initPlanDagWidgets(event.target);
  });
})();
