import { motionPresets, motionTuning, type MotionAction, type MotionKnob, type MotionPreset } from "./motion-tuning"

const global = "Global"

// The areas in the order the panel shows them; any other area follows in the order it registered.
const areas = ["Sidebars", "Timeline", "Composer", "Projects", "Voice"]

/**
 * Mounts the Knobs panel into `container`: the Global card (speed) pinned at the top, then a tab per area of the app with
 * the shipped values and the Snappy, Smooth and Calm presets for every spring at once. Each feature in an area is its
 * own card, folded to one line that shows what's picked, with its description under its title; hovering or focusing a
 * card outlines the part of the app it changes. Experiments carry a marker after their label. knobs loads on first
 * mount, so importing this module costs nothing until then. Works anywhere a surface registered its knobs, including
 * Storybook stories. Returns its removal.
 */
export function mountMotionPanel(container: HTMLElement, options: { title?: string; onClose?: () => void } = {}) {
  let unmount: (() => void) | undefined
  // The preset chosen to leave a mode, applied once the rebuilt panel has the knobs it sets.
  let switching: string | undefined
  let disposed = false

  void start()

  return () => {
    disposed = true
    unmount?.()
  }

  async function start() {
    const [{ action, defineControls }, { applyMotion, motionField }] = await Promise.all([
      import("knobs"),
      import("./motion-tuning-fields"),
    ])

    const show = outline()

    // Knobs whose `when` doesn't hold are left out; a change to it rebuilds the panel like a new knob would.
    const shown = () => motionTuning.list().filter((item) => item.when?.() ?? true)

    const identity = () => [...shown(), ...motionTuning.actions()].map((item) => `${item.group}/${item.id}`).join()

    // Rebuilt when a surface registers or removes a knob or action.
    const mount = () => {
      unmount?.()
      const all = shown()
      // Modes sit in the preset strip, not as rows. While one is on, the spring presets leave its area's springs alone
      // (choosing one switches back to them first); other areas' springs take presets as usual.
      const active = all.find((item) => item.kind === "mode" && item.value)
      const modes = all.filter((item) => item.kind === "mode")
      const list = all.filter((item) => item.kind !== "mode")
      const actions = motionTuning.actions()
      const mounted = identity()
      const springs = list.flatMap((item) => (item.kind === "spring" && item.group !== active?.group ? [item] : []))
      const preset = (id: MotionPreset) => Object.fromEntries(springs.map((item) => [item.id, item.presets[id]]))
      const experiments = new Set(list.flatMap((item) => (item.experiment ? [item.id] : [])))
      // Easing editors open inline, like the spring rows beside them, rather than folded behind a chevron.
      openEditors(list.flatMap((item) => (item.kind === "easing" ? [item.id] : [])))

      const rank = (group: string) => (areas.includes(group) ? areas.indexOf(group) : areas.length)
      const sections = [...new Set([...list, ...actions].map((item) => item.group))]
        .filter((group) => group !== global)
        .toSorted((a, b) => rank(a) - rank(b))

      // A card per feature, in the order its area's features registered; an area's own knobs come first.
      const cards = [...new Set([...list, ...actions].filter((item) => item.group !== global).map(cardOf))]
      const ordered = [...list, ...actions]
        .filter((item) => item.group !== global)
        .toSorted(
          (a, b) =>
            sections.indexOf(a.group) - sections.indexOf(b.group) ||
            Number(!!a.subsection) - Number(!!b.subsection) ||
            cards.indexOf(cardOf(a)) - cards.indexOf(cardOf(b)),
        )

      // What each card explains and outlines: the first of its knobs that says.
      const about = new Map(
        cards.flatMap((card) => {
          const info = list.find((item) => cardOf(item) === card && (item.description || item.target))

          return info ? [[card, info] as const] : []
        }),
      )

      // What a folded card shows: the first choice or switch in it.
      const leads = new Map(
        cards.flatMap((card) => {
          const lead = list.find((item) => cardOf(item) === card && (item.kind === "choice" || item.kind === "toggle"))

          return lead ? [[card, lead] as const] : []
        }),
      )

      const controls = defineControls(
        "opencode-motion",
        {
          ...Object.fromEntries(
            list
              .filter((item) => item.group === global)
              .map((item) => [item.id, motionField(item, { label: item.label, group: global })]),
          ),
          ...Object.fromEntries(
            ordered.map((item) =>
              "run" in item
                ? [
                    `action.${item.id}`,
                    action(item.label, {
                      group: cardOf(item),
                      subsection: "Replay",
                      run: item.run,
                      tab: item.group,
                    }),
                  ]
                : [item.id, motionField(item, { label: item.label, group: cardOf(item), tab: item.group })],
            ),
          ),
        },
        {
          // knobs enables itself only on localhost; this module is only loaded in developer channels.
          enabled: true,
          ship: () => import("knobs/panel"),
          persist: "variants",
          pinned: [global],
          groupLabels: Object.fromEntries(cards.map((card) => [card, cardLabel(card, list)])),
          groupDescriptions: Object.fromEntries(
            [...about].flatMap(([card, info]) => (info.description ? [[card, info.description]] : [])),
          ),
          // Springs ship different presets, so the defaults get their own chip.
          defaultLabel: "Shipped",
          presetLabels: { ...motionPresets, ...Object.fromEntries(modes.map((item) => [item.id, item.label])) },
          presets: {
            snappy: preset("snappy"),
            smooth: preset("smooth"),
            calm: preset("calm"),
            ...Object.fromEntries(modes.map((item) => [item.id, {}])),
          },
          onChange: (patch) =>
            list.forEach((item) => {
              // SAFETY: each field was made from its knob above, so its value is that knob's kind.
              const value = patch[item.id] as MotionKnob["value"] | undefined

              if (value !== undefined) applyMotion(item, value)
            }),
        },
      )

      const sync = () => controls.sync(Object.fromEntries(list.map((item) => [item.id, item.value])))
      sync()

      // The strip shows the mode that's on; a preset chosen while a mode was on applies once its knobs are back.
      if (active && active.id in controls.presets) controls.preset(active.id)

      if (!active && switching && switching in controls.presets) controls.preset(switching)
      switching = undefined

      const stopModes = controls.subscribe(() => {
        const chosen = controls.activePreset()

        modes.forEach((item) => {
          const on = chosen?.kind === "preset" && chosen.id === item.id

          if (on === item.value) return

          if (!on) switching = chosen?.kind === "preset" ? chosen.id : chosen ? undefined : "default"
          motionTuning.set(item, on)
        })
      })

      const stopPanel = controls.mount(container, {
        tabs: sections,
        theme: "auto",
        title: options.title ?? "Knobs",
        onClose: options.onClose,
        // The debug bar shows and hides this panel, and `s` jumps only with focus inside it, so the app keeps its keys.
        toggleKey: false,
        hints: "panel",
        rowAccessory: (key) => (experiments.has(key) ? experimentMark() : null),
        // Every card folds to one line; knobs remembers the ones opened.
        groupFolded: Object.fromEntries(cards.map((card) => [card, true])),
        // What's picked, in the same place on every card. knobs adds the reset button beside it once anything changed.
        groupSummary: (group, info) => {
          const lead = leads.get(group)

          if (!lead) return info.changed ? `${info.changed} changed` : null

          const value = controls.get()[lead.id]

          if (lead.kind === "choice") return lead.labels[String(value)] ?? String(value)

          return value ? "On" : "Off"
        },
        // A die on every card, so the summaries, dice and chevrons each keep one column. A card of switches, which
        // Random leaves alone, flips one of them instead.
        groupRandom: (group) => {
          const keys = list.filter((item) => cardOf(item) === group)

          if (keys.some((item) => item.kind !== "toggle")) return

          const item = keys[Math.floor(Math.random() * keys.length)]

          if (item) controls.set({ [item.id]: Math.random() < 0.5 })

          return true
        },
        // knobs reports a card or row at most once per frame, and null when nothing in the panel is active.
        onActive: (at) =>
          show(at ? (list.find((item) => item.id === at.key)?.target ?? about.get(at.group)?.target) : undefined),
      })

      const stopWatch = motionTuning.subscribe(() => {
        // Edits from elsewhere, such as the debug bar's SLOW, show here too.
        if (identity() === mounted) return sync()
        mount()
      })

      unmount = () => {
        stopWatch()
        stopModes()
        show(undefined)
        stopPanel()
      }
    }

    if (!disposed) mount()
  }
}

// A feature's card: its area and the feature's name, or the area alone for the area's own knobs.
function cardOf(item: MotionKnob | MotionAction) {
  return item.subsection ? `${item.group} · ${item.subsection}` : item.group
}

// A feature card's title. An area's own card would repeat the area's name, so it takes its knob's label when it has one,
// else "General".
function cardLabel(card: string, list: MotionKnob[]) {
  if (card.includes(" · ")) return card.split(" · ").at(-1) ?? card
  const own = list.filter((item) => cardOf(item) === card)

  return own.length === 1 && own[0] ? own[0].label : "General"
}

// Marks a row as a design experiment, in the panel's quietest caption style.
function experimentMark() {
  const mark = document.createElement("span")
  mark.dataset.experiment = ""
  mark.textContent = "Exp"
  mark.title = "Experiment"
  mark.style.cssText = "font-size: var(--_k-text-cap); letter-spacing: 0.06em; color: var(--_k-fg-3)"

  return mark
}

// knobs remembers which rows' editors are open in this tab's session; adding ids there opens them on mount.
function openEditors(ids: string[]) {
  if (ids.length === 0 || typeof sessionStorage === "undefined") return
  const key = "knobs:opencode-motion:session:v1"
  const session = JSON.parse(sessionStorage.getItem(key) ?? "{}")
  const expanded: string[] = Array.isArray(session.expanded) ? session.expanded : []

  if (ids.every((id) => expanded.includes(id))) return
  sessionStorage.setItem(key, JSON.stringify({ ...session, expanded: [...new Set([...expanded, ...ids])] }))
}

// Frames the part of the app a card changes: a pointer-transparent outline that fades in and out. Its rects are read
// once per change of target, never per frame.
function outline() {
  const frames: HTMLElement[] = []
  let current: string | undefined

  const clear = () =>
    frames.splice(0).forEach((frame) => {
      frame.style.opacity = "0"
      setTimeout(() => frame.remove(), 200)
    })

  return (target: string | undefined) => {
    if (target === current) return
    current = target
    clear()

    if (!target) return

    document.querySelectorAll(target).forEach((element) => {
      const rect = element.getBoundingClientRect()

      if (rect.width === 0 || rect.height === 0) return
      const frame = document.body.appendChild(document.createElement("div"))

      frame.style.cssText = `position:fixed;z-index:60;pointer-events:none;opacity:0;transition:opacity .15s;border-radius:6px;
        left:${rect.left - 3}px;top:${rect.top - 3}px;width:${rect.width + 6}px;height:${rect.height + 6}px;
        border:1.5px solid var(--border-interactive-base);background:color-mix(in srgb, var(--surface-interactive-base) 50%, transparent)`
      requestAnimationFrame(() => (frame.style.opacity = "1"))
      frames.push(frame)
    })
  }
}
